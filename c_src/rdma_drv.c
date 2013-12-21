/*
 * rdma_drv.c
 * Copyright (C) 2013 James Lee
 *
 * The contents of this file are subject to the Erlang Public License,
 * Version 1.1, (the "License"); you may not use this file except in
 * compliance with the License. You should have received a copy of the
 * Erlang Public License along with this software. If not, it can be
 * retrieved online at http://www.erlang.org/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 */

#include <arpa/inet.h>
#include <ei.h>
#include <erl_driver.h>
#include <fcntl.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>
#include <string.h>

#include "rdma_drv_buffers.h"
#include "rdma_drv_options.h"

#define DRV_CONNECT    'C'
#define DRV_LISTEN     'L'
#define DRV_ACCEPT     'A'
#define DRV_PEERNAME   'P'
#define DRV_SOCKNAME   'S'
#define DRV_RECV       'R'
#define DRV_DISCONNECT 'D'
#define DRV_GETSTAT    'G'
#define DRV_SETOPTS    'O'
#define DRV_CANCEL     'c'
#define DRV_TIMEOUT    'T'

#define ACK (1 << 31)

typedef enum {
    STATE_DISCONNECTED,
    STATE_CONNECTED,
    STATE_LISTENING,
} RdmaDrvState;

typedef enum {
    ACTION_NONE,
    ACTION_ACCEPTING,
    ACTION_RECEIVING,
    ACTION_DISCONNECTING,
} RdmaDrvAction;

typedef struct rdma_drv_data {
    RdmaDrvState state;
    RdmaDrvAction action;
    RdmaDrvOptions options;
    ErlDrvPort port;
    ErlDrvTermData caller;

    /* rdma connection manager stuff */
    struct rdma_cm_id *id;
    struct rdma_event_channel *ec;

    /* ibverbs stuff */
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    struct ibv_mr *send_mr, *recv_mr;
    RdmaDrvBuffers send_buffers, recv_buffers;

    /* place to accumulate packet that takes more than one recv */    
    void *incomplete_recv;
    int incomplete_recv_offset;

    /* basic flow control data */
    bool sending_ack;
    int pending_acks;
    int peer_ready;

    /* stats */
    unsigned long sent;
    unsigned long received;
    unsigned long buffered;

    /* linked list for holding accepted sockets */
    struct rdma_drv_data *listener; /* (head) */
    struct rdma_drv_data *next;
    ErlDrvMutex *list_mutex;
} RdmaDrvData;

static void rdma_drv_send_error_atom(RdmaDrvData *data, char *str) {
    if (data->options.active) {
        /* dist_util seems to expect {tcp_closed, Socket} */
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM, driver_mk_atom("tcp_closed"),
            ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
    } else {
        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
                ERL_DRV_ATOM, driver_mk_atom("error"),
                ERL_DRV_ATOM, driver_mk_atom(str),
                ERL_DRV_TUPLE, 2,
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));
    }
}

static void rdma_drv_pause(RdmaDrvData *data) {
    data->action = ACTION_NONE;

    if (data->ec) {
        driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 0);
    }

    if (data->comp_channel) {
        driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 0);
    }
}

static void rdma_drv_resume(RdmaDrvData *data) {
    /* Save who to reply to. */
    data->caller = driver_caller(data->port);

    if (data->ec) {
        driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 1);
    }

    if (data->comp_channel) {
        driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 1);
    }
}

static bool rdma_drv_add_data(RdmaDrvData *data, RdmaDrvData *add) {
    if (!data->list_mutex) {
        data->list_mutex = erl_drv_mutex_create("list_mutex");
        if (!data->list_mutex) {
            return false;
        }
    }

    erl_drv_mutex_lock(data->list_mutex);
    add->next = data->next;
    data->next = add;
    erl_drv_mutex_unlock(data->list_mutex);

    return true;
}

static bool rdma_drv_remove_data(RdmaDrvData *data, RdmaDrvData *remove) {
    RdmaDrvData *i;
    bool ret = false;

    if (!data->list_mutex) {
        return false;
    }

    erl_drv_mutex_lock(data->list_mutex);

    for (i = data; i->next != NULL; i = i->next) {
        if (i->next == remove) {
            i->next = i->next->next;
            ret = true;
            break;
        }
    }

    erl_drv_mutex_unlock(data->list_mutex);

    return ret;
}

static void rdma_drv_post_ack(RdmaDrvData *data) {
    int ret;
    struct ibv_send_wr wr = {}, *bad_wr = NULL;

    if (data->sending_ack || data->pending_acks == 0) {
        return;
    }

    wr.wr_id = ACK;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(ACK | data->pending_acks);

    ret = ibv_post_send(data->id->qp, &wr, &bad_wr);
    if (ret) {
        rdma_drv_send_error_atom(data, "ibv_post_send");
        return;
    }

    data->sending_ack = true;
    data->pending_acks = 0;
}

static void rdma_drv_post_recv(RdmaDrvData *data, void *buffer) {
    int ret;
    struct ibv_recv_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) buffer;
    sge.length = data->options.buffer_size;
    sge.lkey = data->recv_mr->lkey;

    ret = ibv_post_recv(data->id->qp, &wr, &bad_wr);
    if (ret) {
        rdma_drv_send_error_atom(data, "ibv_post_recv");
        return;
    }
}

static void rdma_drv_post_send(RdmaDrvData *data, void *buffer, ErlDrvSizeT remaining) {
    int ret;
    struct ibv_send_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(remaining);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) buffer;
    sge.length = remaining < data->options.buffer_size ? remaining : data->options.buffer_size;
    sge.lkey = data->send_mr->lkey;

    ret = ibv_post_send(data->id->qp, &wr, &bad_wr);
    if (ret) {
        rdma_drv_send_error_atom(data, "ibv_post_send");
        return;
    }

    data->peer_ready--;
}

static bool rdma_drv_init_ibverbs(RdmaDrvData *data) {
    int ret;

    /* Allocate a protection domain. */
    data->pd = ibv_alloc_pd(data->id->verbs);
    if (!data->pd) {
        rdma_drv_send_error_atom(data, "ibv_alloc_pd");
        return false;
    }

    /* Create a completion event channel. */
    data->comp_channel = ibv_create_comp_channel(data->id->verbs);
    if (!data->comp_channel) {
        rdma_drv_send_error_atom(data, "ibv_create_comp_channel");
        return false;
    }

    /* Make the completion event channel non-blocking. */
    fcntl(data->comp_channel->fd, F_SETFL, fcntl(data->comp_channel->fd, F_GETFL) | O_NONBLOCK);

    /*
     * Create a completion queue large enough to hold all of the work
     * items that could be placed in the send and receive queues.
     */
    data->cq = ibv_create_cq(data->id->verbs, data->options.num_buffers * 2, NULL, data->comp_channel, 0);
    if (!data->cq) {
        rdma_drv_send_error_atom(data, "ibv_create_cq");
        return false;
    }
    
    /* Request a completion queue event in the completion channel. */
    ret = ibv_req_notify_cq(data->cq, 0);
    if (ret) {
        rdma_drv_send_error_atom(data, "ibv_req_notify_cq");
        return false;
    }

    /* Initialize the send and receive buffers. */
    ret = rdma_drv_buffers_init(&data->send_buffers, data->options.buffer_size, data->options.num_buffers - 1);
    if (!ret) {
        rdma_drv_send_error_atom(data, "rdma_drv_buffers_init_send");
        return false;
    }

    ret = rdma_drv_buffers_init(&data->recv_buffers, data->options.buffer_size, data->options.num_buffers);
    if (!ret) {
        rdma_drv_send_error_atom(data, "rdma_drv_buffers_init_recv");
        return false;
    }

    /* Register the buffers with the RDMA device. */
    data->send_mr = ibv_reg_mr(data->pd, data->send_buffers.buffers, data->options.buffer_size * (data->options.num_buffers - 1), IBV_ACCESS_LOCAL_WRITE);
    if (!data->send_mr) {
        rdma_drv_send_error_atom(data, "ibv_reg_mr_send");
        return false;
    }

    data->recv_mr = ibv_reg_mr(data->pd, data->recv_buffers.buffers, data->options.buffer_size * data->options.num_buffers, IBV_ACCESS_LOCAL_WRITE);
    if (!data->recv_mr) {
        rdma_drv_send_error_atom(data, "ibv_reg_mr_recv");
        return false;
    }

    /* Create the queue pair. */
    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.send_cq = data->cq;
    qp_attr.recv_cq = data->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = data->options.num_buffers;
    qp_attr.cap.max_recv_wr = data->options.num_buffers;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    ret = rdma_create_qp(data->id, data->pd, &qp_attr);
    if (ret) {
        rdma_drv_send_error_atom(data, "rdma_create_qp");
        return false;
    }

    /* Post all available receive buffers. */
    void *recv_buffer;
    while ((recv_buffer = rdma_drv_buffers_reserve_buffer(&data->recv_buffers))) {
        rdma_drv_post_recv(data, recv_buffer);
    }

    /*
     * Assume that the peer has posted the same number of receive work
     * items as we have posted, minus one, which is "reserved" for ACKs
     */
    data->peer_ready = data->options.num_buffers - 1;

    return true;
}

static void rdma_drv_free_ibverbs(RdmaDrvData *data) {
    /*
     * Basically 'rdma_drv_init_ibverbs' in reverse.
     */

    if (data->id && data->id->qp) {
        rdma_destroy_qp(data->id);
    }

    if (data->recv_mr) {
        ibv_dereg_mr(data->recv_mr);
        data->recv_mr = NULL;
    }

    if (data->send_mr) {
        ibv_dereg_mr(data->send_mr);
        data->send_mr = NULL;
    }

    rdma_drv_buffers_free(&data->recv_buffers);
    rdma_drv_buffers_free(&data->send_buffers);

    if (data->cq) {
        ibv_destroy_cq(data->cq);
        data->cq = NULL;
    }

    if (data->comp_channel) {
        ibv_destroy_comp_channel(data->comp_channel);
        data->comp_channel = NULL;
    }

    if (data->pd) {
        ibv_dealloc_pd(data->pd);
        data->pd = NULL;
    }
}

static ErlDrvData rdma_drv_start(ErlDrvPort port, char *command) {
    RdmaDrvData *data = (RdmaDrvData *) driver_alloc(sizeof(RdmaDrvData));
    if (!data) {
        return NULL;
    }

    memset(data, 0, sizeof(RdmaDrvData));
    data->port = port;

    /* ei is used in the control interface. */
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData) data;
}

static void rdma_drv_stop(ErlDrvData drv_data) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data, *i;

    /* Stop polling event channels. */
    rdma_drv_pause(data);

    /* Kill child sockets. */
    if (data->list_mutex) {
        erl_drv_mutex_lock(data->list_mutex);
        for (i = data; i->next != NULL; i = i->next) {
            rdma_drv_send_error_atom(i->next, "closed");
            driver_failure_eof(i->next->port); 
        }
        data->next = NULL;
        erl_drv_mutex_unlock(data->list_mutex);
        erl_drv_mutex_destroy(data->list_mutex);
        data->list_mutex = NULL;
    }

    rdma_drv_free_ibverbs(data); 

    if (data->id) {
        rdma_destroy_id(data->id);
    }

    if (data->ec) {
        rdma_destroy_event_channel(data->ec);
    }

    driver_free(data);
}

static ErlDrvSizeT rdma_drv_send_fully(RdmaDrvData *data, void *buf, ErlDrvSizeT len, bool buf_is_vec) {
    void *send_buffer;
    ErlDrvSizeT remaining = len;

    while (data->peer_ready && (send_buffer = rdma_drv_buffers_reserve_buffer(&data->send_buffers))) {
        ErlDrvSizeT send_amount = remaining < data->options.buffer_size ? remaining : data->options.buffer_size;

        if (buf_is_vec) {
            ErlIOVec *queue = (ErlIOVec *) buf;
            driver_vec_to_buf(queue, send_buffer, send_amount);
            driver_deq(data->port, send_amount);
            driver_peekqv(data->port, queue);
        } else {
            memcpy(send_buffer, buf, send_amount);
            buf += send_amount;
        }

        rdma_drv_post_send(data, send_buffer, remaining);
        remaining -= send_amount;

        if (remaining == 0) {
            /* Increment the stats counter. */
            data->sent++;

            break;
        }
    }

    return remaining;
}

static bool rdma_drv_flush_queue(RdmaDrvData *data) {
    ErlIOVec queue;

    while (driver_peekqv(data->port, &queue)) {
        ErlDrvSizeT remaining;
        driver_vec_to_buf(&queue, (char *) &remaining, sizeof(remaining));
        driver_deq(data->port, sizeof(remaining));
        driver_peekqv(data->port, &queue);

        remaining = rdma_drv_send_fully(data, &queue, remaining, true);
        if (remaining > 0) {
            driver_pushq(data->port, (char *) &remaining, sizeof(remaining));
            return false;
        } else {
            /*
             * This is where we know a packet was fully removed from
             * the queue.
             */
            data->buffered--;
        }
    }

    return true;
}

static void rdma_drv_output(ErlDrvData drv_data, char *buf, ErlDrvSizeT len) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    /* Save who to reply to. */
    data->caller = driver_caller(data->port);

    if (data->state != STATE_CONNECTED) {
        rdma_drv_send_error_atom(data, "not_connected");
        return;
    }

    if (rdma_drv_flush_queue(data)) {
        ErlDrvSizeT remaining = rdma_drv_send_fully(data, buf, len, false);
        if (remaining > 0) {
            driver_enq(data->port, (char *) &remaining, sizeof(remaining));
            driver_enq(data->port, buf + (len - remaining), remaining);

            /* Packets get put into the queue here... */
            data->buffered++;
        }
    } else {
        driver_enq(data->port, (char *) &len, sizeof(len));
        driver_enq(data->port, buf, len);

        /* ...and here. */
        data->buffered++;
    }
}

static void rdma_drv_handle_rdma_cm_event_connect_request(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    int ret;
    ErlDrvPort new_port;
    RdmaDrvData *new_data;
    struct rdma_conn_param cm_params = {};

    /*
     * We are going to make a new port for the accepted socket.
     */

    new_data = (RdmaDrvData *) driver_alloc(sizeof(RdmaDrvData));
    if (!new_data) {
        rdma_drv_send_error_atom(data, "driver_alloc");
        return;
    }

    memset(new_data, 0, sizeof(RdmaDrvData));
    new_port = driver_create_port(data->port, data->caller, "rdma_drv", (ErlDrvData) new_data);

    /* ei is used in the control interface. */
    set_port_control_flags(new_port, PORT_CONTROL_FLAG_BINARY);

    /*
     * Connect the new port data to the listener so it can be closed
     * if the listener decides to first.
     */
    new_data->listener = data;
    ret = rdma_drv_add_data(data, new_data);
    if (!ret) {
        rdma_drv_send_error_atom(data, "rdma_drv_add_data");
        return;
    }

    new_data->id = cm_event->id;
    new_data->port = new_port;
    new_data->options = data->options;

    /* Send the port to Erlang. */
    ErlDrvTermData spec[] = {
        ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_ATOM, driver_mk_atom("port"),
            ERL_DRV_PORT, driver_mk_port(new_port),
            ERL_DRV_TUPLE, 2,
        ERL_DRV_TUPLE, 2,
    };

    erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));

    /*
     * For better or worse, events related to the new socket still come
     * in on the listener socket, so we have to store the new data
     * somewhere it can be accessed...see:
     *   rdma_drv_handle_rdma_cm_event_established
     *   rdma_drv_handle_rdma_cm_event_disconnected
     */
    new_data->id->context = new_data;

    /* If ibverbs are initialized successfully, accept the connection. */
    if (rdma_drv_init_ibverbs(new_data)) {
        ret = rdma_accept(new_data->id, &cm_params);
        if (ret) {
            rdma_drv_send_error_atom(data, "rdma_accept");
            return;
        }
    }
}

static void rdma_drv_handle_rdma_cm_event_addr_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    int ret;

    ret = rdma_resolve_route(data->id, data->options.timeout / 4);
    if (ret) {
        rdma_drv_send_error_atom(data, "rdma_resolve_route");
        return;
    }
}

static void rdma_drv_handle_rdma_cm_event_route_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    int ret;
    struct rdma_conn_param cm_params = {};

    if (rdma_drv_init_ibverbs(data)) {
        ret = rdma_connect(data->id, &cm_params);
        if (ret) {
            rdma_drv_send_error_atom(data, "rdma_connect");
            return;
        }
    }
}

static void rdma_drv_handle_rdma_cm_event_established(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    if (cm_event->id->context) {
        RdmaDrvData *new_data = (RdmaDrvData *) cm_event->id->context;
        new_data->state = STATE_CONNECTED;

        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
                ERL_DRV_ATOM, driver_mk_atom("accept"),
                ERL_DRV_PORT, driver_mk_port(new_data->port),
                ERL_DRV_TUPLE, 2,
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));

        if (new_data->options.active) {
            /*
             * If the listener was configured to be "active", we want
             * to start polling the accepted socket for recvs.
             */
            rdma_drv_resume(new_data);
        }

        /* We've completed an accept, so stop polling. */
        rdma_drv_pause(data);
    } else {
        data->state = STATE_CONNECTED;

        /* Stop polling for events unless this is an active socket. */
        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_ATOM, driver_mk_atom("established"),
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));

        if (!data->options.active) {
            rdma_drv_pause(data);
        }
    }
}

static void rdma_drv_handle_rdma_cm_event_disconnected(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    int ret;

    if (cm_event->id->context) {
        /*
         * When event occurs in listener, it actually refers to a
         * previously accepted socket.
         */
        data = (RdmaDrvData *) cm_event->id->context;

        /* Unlink socket from the listener. */
        rdma_drv_remove_data(data->listener, data);
    }

    if (data->action == ACTION_DISCONNECTING) {
        /*
         * This event is the result of calling rdma_disconnect earlier,
         * which is to say, it is expected.
         */
        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_ATOM, driver_mk_atom("disconnected"),
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));
    } else {
        /*
         * This event is the result of the peer calling
         * rdma_disconnect, which is to say, it is unexpected.
         */
        ret = rdma_disconnect(data->id);
        if (ret) {
            rdma_drv_send_error_atom(data, "rdma_disconnect");
            return;
        }

        rdma_drv_send_error_atom(data, "closed");
    }

    /* Empty the queue (otherwise the port won't close). */
    /* XXX: Technically, this should require a port data lock. */
    driver_deq(data->port, driver_sizeq(data->port));

    /* Either way, if we get to this point, we are disconnected. */
    data->state = STATE_DISCONNECTED;
}

static void rdma_drv_handle_rdma_cm_event(RdmaDrvData *data) {
    int ret;
    struct rdma_cm_event *cm_event;

    ret = rdma_get_cm_event(data->ec, &cm_event);
    if (ret) {
        rdma_drv_send_error_atom(data, "cm_get_event_failed");
        return;
    }

    switch (cm_event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            rdma_drv_handle_rdma_cm_event_connect_request(data, cm_event);
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
            rdma_drv_send_error_atom(data, "connection_failed");
            break;

        case RDMA_CM_EVENT_ADDR_RESOLVED:
            rdma_drv_handle_rdma_cm_event_addr_resolved(data, cm_event);
            break;

        case RDMA_CM_EVENT_ADDR_ERROR:
            rdma_drv_send_error_atom(data, "address_resolution_failed");
            break;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            rdma_drv_handle_rdma_cm_event_route_resolved(data, cm_event);
            break;

        case RDMA_CM_EVENT_ROUTE_ERROR:
            rdma_drv_send_error_atom(data, "route_resolution_failed");
            break;

        case RDMA_CM_EVENT_UNREACHABLE:
            rdma_drv_send_error_atom(data, "unreachable");
            break;

        case RDMA_CM_EVENT_REJECTED:
            rdma_drv_send_error_atom(data, "rejected");
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            rdma_drv_handle_rdma_cm_event_established(data, cm_event);
            break;

        case RDMA_CM_EVENT_DISCONNECTED:
            rdma_drv_handle_rdma_cm_event_disconnected(data, cm_event);
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            /* XXX: Should we do anything here? */
            break;

        default:
            rdma_drv_send_error_atom(data, "unhandled_event");
            break;
    }

    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
        rdma_drv_send_error_atom(data, "cm_ack_event_failed");
        return;
    }
}

static bool rdma_drv_handle_recv_complete(RdmaDrvData *data, struct ibv_wc *wc) {
    bool completed = false;
    uint32_t imm_data = ntohl(wc->imm_data);
    void *recv_buffer = rdma_drv_buffers_current_buffer(&data->recv_buffers);

    if (imm_data & ACK) {
        data->peer_ready += imm_data & ~ACK;
        rdma_drv_post_recv(data, recv_buffer);
        rdma_drv_flush_queue(data);
    } else {
        ErlDrvSizeT remaining = imm_data;
        ErlDrvSizeT recv_amount = remaining < data->options.buffer_size ? remaining : data->options.buffer_size;

        if (remaining > data->options.buffer_size && !data->incomplete_recv) {
            data->incomplete_recv = driver_alloc(remaining);
            data->incomplete_recv_offset = 0;

            if (!data->incomplete_recv) {
                rdma_drv_send_error_atom(data, "driver_alloc_recv");
                return false;
            }
        }

        if (data->incomplete_recv) {
            memcpy(data->incomplete_recv + data->incomplete_recv_offset, recv_buffer, recv_amount);
            data->incomplete_recv_offset += recv_amount;
        }

        if (remaining <= data->options.buffer_size) {
            void *output_buffer;
            ErlDrvSizeT output_buffer_size;

            if (data->incomplete_recv) {
                output_buffer = data->incomplete_recv;
                output_buffer_size = data->incomplete_recv_offset;
            } else {
                output_buffer = recv_buffer;
                output_buffer_size = remaining;
            }

            /* Send packet to emulator. */
            if (data->options.active) {
                /* Based on 'inet_port_data' from 'inet_drv.c' */
                if (!data->options.binary || data->options.packet > output_buffer_size) {
                    driver_output2(data->port, output_buffer, output_buffer_size, NULL, 0);
                } else if (data->options.packet > 0) {
                    driver_output2(data->port, output_buffer, data->options.packet, output_buffer + data->options.packet, output_buffer_size - data->options.packet);
                } else {
                    driver_output(data->port, output_buffer, output_buffer_size);
                }
            } else {
                ErlDrvTermData spec[] = {
                    ERL_DRV_PORT, driver_mk_port(data->port),
                        ERL_DRV_ATOM, driver_mk_atom("data"),
                        data->options.binary ? ERL_DRV_BUF2BINARY : ERL_DRV_STRING, (ErlDrvTermData) output_buffer, output_buffer_size,
                        ERL_DRV_TUPLE, 2,
                    ERL_DRV_TUPLE, 2,
                };

                erl_drv_send_term(driver_mk_port(data->port), data->caller, spec, sizeof(spec) / sizeof(spec[0]));
            }

            if (!data->options.active) {
                /* Stop polling for events. */
                rdma_drv_pause(data);

                /* Return false so we don't process any more cq entries. */
                completed = true;
            }

            if (data->incomplete_recv) {
                driver_free(data->incomplete_recv);
                data->incomplete_recv = NULL;
                data->incomplete_recv_offset = 0;
            }

            /* Increment the stats counter. */
            data->received++;
        }

        rdma_drv_post_recv(data, recv_buffer);
        data->pending_acks++;
    }

    return completed;
}

static void rdma_drv_handle_send_complete(RdmaDrvData *data, struct ibv_wc *wc) {
    if (wc->wr_id == ACK) {
        data->sending_ack = false;
    } else {
        rdma_drv_buffers_release_buffer(&data->send_buffers);
        rdma_drv_flush_queue(data);
    }
}

static bool rdma_drv_flush_cq(RdmaDrvData *data) {
    bool completed = false;
    struct ibv_wc wc;

    while (!completed && ibv_poll_cq(data->cq, 1, &wc)) {
        if (wc.status == IBV_WC_SUCCESS) {
            if (wc.opcode == IBV_WC_RECV) {
                completed = rdma_drv_handle_recv_complete(data, &wc);
            } else if (wc.opcode == IBV_WC_SEND) {
                rdma_drv_handle_send_complete(data, &wc);
            }
        }

        /* XXX: What to do about unsuccessful work items? */
    }

    rdma_drv_post_ack(data);

    /* Queue flushed completely. */
    return completed;
}

static void rdma_drv_handle_comp_channel_event(RdmaDrvData *data) {
    void *ev_ctx;
    struct ibv_cq *cq;

    /* XXX: Check error status of these functions. */
    ibv_get_cq_event(data->comp_channel, &cq, &ev_ctx);
    ibv_ack_cq_events(cq, 1);

    /* Request a completion queue event in the completion channel. */
    ibv_req_notify_cq(cq, 0);

    rdma_drv_flush_cq(data);
}

static void rdma_drv_ready_input(ErlDrvData drv_data, ErlDrvEvent event) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    if (data->ec && event == (ErlDrvEvent) data->ec->fd) {
        rdma_drv_handle_rdma_cm_event(data);
    } else if (data->comp_channel && event == (ErlDrvEvent) data->comp_channel->fd) {
        rdma_drv_handle_comp_channel_event(data);
    }
}

static void rdma_drv_encode_error_string(ei_x_buff *x, const char *str) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "error");
    ei_x_encode_string(x, str);
}

static void rdma_drv_encode_error_posix(ei_x_buff *x, int error) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "error");
    ei_x_encode_atom(x, erl_errno_id(error));
}

static void rdma_drv_encode_error_atom(ei_x_buff *x, const char *str) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "error");
    ei_x_encode_atom(x, str);
}

static void rdma_drv_control_connect(RdmaDrvData *data, char *buf, ei_x_buff *x) {
    int ret;

    /* Parse the connection options */
    rdma_drv_options_init(&data->options);
    if (!rdma_drv_options_parse(&data->options, buf)) {
        rdma_drv_encode_error_atom(x, "bad_options");
        return;
    }

    /* Resolve the address and port */
    struct addrinfo *addr;
    struct addrinfo *hints = NULL;
    ret = getaddrinfo(data->options.dest_host, data->options.dest_port, hints, &addr);
    if (ret == EAI_SYSTEM) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    } else if (ret) {
        rdma_drv_encode_error_atom(x, "getaddrinfo");
        return;
    }

    /* Create the event channel */
    data->ec = rdma_create_event_channel();
    if (!data->ec) {
        rdma_drv_encode_error_string(x, "rdma_create_event_channel");
        return;
    }

    /* Make event channel non-blocking channel. */
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);

    /* Create the "socket" */
    ret = rdma_create_id(data->ec, &data->id, NULL, RDMA_PS_TCP);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Start address resolution */
    struct sockaddr_in src_addr = {};
    src_addr.sin_family = AF_INET;
    src_addr.sin_port = htons(data->options.port);

    if (data->options.ip[0] && !inet_pton(AF_INET, data->options.ip, &src_addr.sin_addr)) {
        rdma_drv_encode_error_string(x, "inet_pton");
        return;
    }

    ret = rdma_resolve_addr(data->id, (struct sockaddr *) &src_addr, addr->ai_addr, data->options.timeout / 4);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    freeaddrinfo(addr);

    /* Start polling for events. */
    rdma_drv_resume(data);

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_listen(RdmaDrvData *data, char *buf, ei_x_buff *x) {
    int ret;

    /* Parse the connection options */
    rdma_drv_options_init(&data->options);
    if (!rdma_drv_options_parse(&data->options, buf)) {
        rdma_drv_encode_error_atom(x, "bad_options");
        return;
    }

    /* Create the event channel */
    data->ec = rdma_create_event_channel();
    if (!data->ec) {
        rdma_drv_encode_error_string(x, "rdma_create_event_channel");
        return;
    }  

    /* Make event channel non-blocking channel. */
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);

    /* Create the "socket" */
    ret = rdma_create_id(data->ec, &data->id, NULL, RDMA_PS_TCP);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Bind to a specified port */
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(data->options.port);

    if (data->options.ip[0] && !inet_pton(AF_INET, data->options.ip, &addr.sin_addr)) {
        rdma_drv_encode_error_string(x, "inet_pton");
        return;
    }

    ret = rdma_bind_addr(data->id, (struct sockaddr *) &addr);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Start listening for incoming connections */
    ret = rdma_listen(data->id, data->options.backlog);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    data->state = STATE_LISTENING;
    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_accept(RdmaDrvData *data, ei_x_buff *x) {
/*
    if (data->action == ACTION_ACCEPTING && driver_caller(data->port) != data->caller) {
        rdma_drv_encode_error_atom(x, "already_accepting");
        return;
    }

    if (data->state != STATE_LISTENING) {
        rdma_drv_encode_error_atom(x, "not_listening");
        return;
    }
*/
    data->action = ACTION_ACCEPTING;

    /* Start polling. */
    rdma_drv_resume(data);

    ei_x_encode_atom(x, "ok");
}    

static void rdma_drv_control_peername(RdmaDrvData *data, ei_x_buff *x) {
    struct sockaddr_in *addr = (struct sockaddr_in *) rdma_get_peer_addr(data->id);

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_string(x, inet_ntoa(addr->sin_addr));
    ei_x_encode_ulong(x, ntohs(rdma_get_dst_port(data->id)));
}

static void rdma_drv_control_sockname(RdmaDrvData *data, ei_x_buff *x) {
    struct sockaddr_in *addr = (struct sockaddr_in *) rdma_get_local_addr(data->id);

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_string(x, inet_ntoa(addr->sin_addr));
    ei_x_encode_ulong(x, ntohs(rdma_get_src_port(data->id)));
}

static void rdma_drv_control_recv(RdmaDrvData *data, ei_x_buff *x) {
    if (data->options.active) {
        if (driver_caller(data->port) != driver_connected(data->port)) {
            rdma_drv_encode_error_atom(x, "not_owner");
            return;
        }
    } else {
        if (data->action == ACTION_RECEIVING) {
            rdma_drv_encode_error_atom(x, "already_receiving");
            return;
        }

        if (data->state == STATE_DISCONNECTED) {
            rdma_drv_encode_error_atom(x, "closed");
            return;
        }

        if (data->state != STATE_CONNECTED) {
            rdma_drv_encode_error_atom(x, "not_connected");
            return;
        }

        /*
         * Caller needs to be set in case rdma_drv_flush_cq needs to
         * output anytihng.
         */
        data->caller = driver_caller(data->port);
        if (!rdma_drv_flush_cq(data)) {
            /* Flushing did not complete a receive, so start polling. */
            data->action = ACTION_RECEIVING;
            rdma_drv_resume(data);
        }
    }

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_disconnect(RdmaDrvData *data, ei_x_buff *x) {
    int ret;

    /*
     * We only need to initiate a proper disconnection for fully
     * connected sockets. Listeners or other sockets in the process
     * of disconnecting don't need to do anything.
     */

    if (data->state == STATE_CONNECTED && data->action != ACTION_DISCONNECTING) {
        /* Start polling for disconnection. */
        rdma_drv_resume(data);

        data->action = ACTION_DISCONNECTING;

        ret = rdma_disconnect(data->id);
        if (ret) {
            rdma_drv_encode_error_posix(x, errno);
            return;
        }

        ei_x_encode_atom(x, "wait");
        return;
    }

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_getstat(RdmaDrvData *data, ei_x_buff *x) {
    ei_x_encode_tuple_header(x, 4);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_ulong(x, data->received);
    ei_x_encode_ulong(x, data->sent);
    ei_x_encode_ulong(x, data->buffered);
}

static void rdma_drv_control_setopts(RdmaDrvData *data, char *buf, ei_x_buff *x) {
    /* Parse the connection options */
    if (!rdma_drv_options_parse(&data->options, buf)) {
        rdma_drv_encode_error_atom(x, "bad_options");
        return;
    }

    if (data->options.active) {
        /* Start polling. */
        rdma_drv_resume(data);
    } else {
        /* Stop polling. */
        rdma_drv_resume(data);
    }

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_cancel(RdmaDrvData *data, ei_x_buff *x) {
    if (data->action != ACTION_NONE) {
        rdma_drv_send_error_atom(data, "canceled");
        rdma_drv_pause(data);
    }

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_timeout(RdmaDrvData *data, ei_x_buff *x) {
    /* Basically the same as cancel, without sending error. */

    if (data->action != ACTION_NONE) {
        rdma_drv_pause(data);
    }

    ei_x_encode_atom(x, "ok");
}

static ErlDrvBinary * ei_x_to_new_binary(ei_x_buff *x) {
    ErlDrvBinary *bin = driver_alloc_binary(x->index);

    if (bin != NULL)
        memcpy(&bin->orig_bytes[0], x->buff, x->index);

    return bin;
}

static ErlDrvSSizeT rdma_drv_control(ErlDrvData drv_data, unsigned int command, char *buf, ErlDrvSizeT len, char **rbuf, ErlDrvSizeT rlen) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    ei_x_buff x;
    ei_x_new_with_version(&x);

    switch (command) {
        case DRV_CONNECT:
            rdma_drv_control_connect(data, buf, &x);
            break;

        case DRV_LISTEN:
            rdma_drv_control_listen(data, buf, &x);
            break;

        case DRV_ACCEPT:
            rdma_drv_control_accept(data, &x);
            break;

        case DRV_PEERNAME:
            rdma_drv_control_peername(data, &x);
            break;

        case DRV_SOCKNAME:
            rdma_drv_control_sockname(data, &x);
            break;

        case DRV_RECV:
            rdma_drv_control_recv(data, &x);
            break;

        case DRV_DISCONNECT:
            rdma_drv_control_disconnect(data, &x);
            break;

        case DRV_GETSTAT:
            rdma_drv_control_getstat(data, &x);
            break;

        case DRV_SETOPTS:
            rdma_drv_control_setopts(data, buf, &x);
            break;

        case DRV_CANCEL:
            rdma_drv_control_cancel(data, &x);
            break;

        case DRV_TIMEOUT:
            rdma_drv_control_timeout(data, &x);
            break;

        default:
            return -1;
    }

    *rbuf = (char *) ei_x_to_new_binary(&x);
    ei_x_free(&x);

    return 0;
}

static ErlDrvEntry rdma_drv_entry = {
    NULL,                            /* init, N/A */
    rdma_drv_start,                  /* start, called when port is opened */
    rdma_drv_stop,                   /* stop, called when port is closed */
    rdma_drv_output,                 /* output, called when erlang has sent */
    rdma_drv_ready_input,            /* ready_input, called when input descriptor ready */
    NULL,                            /* ready_output, called when output descriptor ready */
    "rdma_drv",                      /* char *driver_name, the argument to open_port */
    NULL,                            /* finish, called when unloaded */
    NULL,                            /* void * that is not used (BC) */
    rdma_drv_control,                /* control, port_control callback */
    NULL,                            /* timeout, called on timeouts */
    NULL,                            /* outputv, vector output interface */
    NULL,                            /* ready_async callback */
    NULL,                            /* flush callback */
    NULL,                            /* call callback */
    NULL,                            /* event callback */
    ERL_DRV_EXTENDED_MARKER,         /* Extended driver interface marker */
    ERL_DRV_EXTENDED_MAJOR_VERSION,  /* Major version number */
    ERL_DRV_EXTENDED_MINOR_VERSION,  /* Minor version number */
    ERL_DRV_FLAG_SOFT_BUSY |         /* Driver flags. Soft busy flag is required for distribution drivers */
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL,                            /* Reserved for internal use */
    NULL,                            /* process_exit callback */
    NULL                             /* stop_select callback */
};

DRIVER_INIT(rdma_drv) {
    return &rdma_drv_entry;
}
