#include <sys/time.h>
#include <stdbool.h>
#include <string.h>
#include <erl_driver.h>
#include <ei.h>
#include <rdma/rdma_cma.h>
#include <fcntl.h>
#include <arpa/inet.h>

#define DRV_CONNECT 'C'
#define DRV_LISTEN  'L'
#define DRV_PORT    'P'
#define DRV_TIME    'T'

#define ACK (1 << 31)

typedef enum {
    // XXX: refactor as STATE_
    DISCONNECTED,
    CONNECTED,
    LISTENING,
} RdmaDrvState;

typedef struct {
    bool binary;
    int backlog;
    short port;
    struct in_addr ip;
    int buffer_size;
    int num_buffers;
    char dest_host[NI_MAXHOST];
    char dest_port[NI_MAXSERV];
    int timeout;
} RdmaDrvConnectionOptions;

typedef struct {
    void *buffers;
    int buffer_size;
    int num_buffers;
    int i;
    int free_i;
    int busy_i;
} RdmaDrvBuffers;

typedef struct {
    RdmaDrvState state;
    RdmaDrvConnectionOptions connection_options;
    ErlDrvPort port;

    struct rdma_cm_id *id;
    struct rdma_event_channel *ec;

    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    struct ibv_mr *send_mr, *recv_mr;
    RdmaDrvBuffers send_buffers, recv_buffers;
    
    void *incomplete_recv;
    int incomplete_recv_offset;

    bool sending_ack;
    int pending_acks;
    int peer_ready;

    struct timeval time_start;
    int op_time;
} RdmaDrvData;

static void rdma_drv_send_error_atom(RdmaDrvData *data, char *str) {
    ErlDrvTermData spec[] = {
        ERL_DRV_PORT, driver_mk_port(data->port),
        ERL_DRV_ATOM, driver_mk_atom("error"),
        ERL_DRV_ATOM, driver_mk_atom(str),
        ERL_DRV_TUPLE, 3,
    };

    erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
}

static void rdma_drv_connection_options_init(RdmaDrvConnectionOptions *connection_options) {
    connection_options->binary = false;
    connection_options->backlog = 5;
    connection_options->port = 0;
    inet_pton(AF_INET, "0.0.0.0", &connection_options->ip);
    connection_options->buffer_size = 1024;
    connection_options->num_buffers = 100;
}

static bool rdma_drv_connection_options_parse_atom(RdmaDrvConnectionOptions *connection_options, char *buf, int *index) {
    char atom[MAXATOMLEN];

    if (ei_decode_atom(buf, index, atom) == 0) {
        if (strcmp(atom, "list") == 0) {
            connection_options->binary = false;
        } else if (strcmp(atom, "binary") == 0) {
            connection_options->binary = true;
        } else {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

static bool rdma_drv_connection_options_parse_tuple(RdmaDrvConnectionOptions *connection_options, char *buf, int *index) {
    int arity = 0;

    if (ei_decode_tuple_header(buf, index, &arity) == 0) {
        char atom[MAXATOMLEN];

        if (ei_decode_atom(buf, index, atom) == 0) {
            if (strcmp(atom, "backlog") == 0) {
                long backlog = 0;
                if (ei_decode_long(buf, index, &backlog) == 0) {
                    connection_options->backlog = (int) backlog;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "port") == 0) {
                long port = 0;
                if (ei_decode_long(buf, index, &port) == 0) {
                    connection_options->port = (short) port;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "ip") == 0) {
                char ip[INET6_ADDRSTRLEN];
                if (ei_decode_string(buf, index, ip) == 0) {
                    if (!inet_pton(AF_INET, ip, &connection_options->ip)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (strcmp(atom, "buffer_size") == 0) {
                long buffer_size = 0;
                if (ei_decode_long(buf, index, &buffer_size) == 0) {
                    connection_options->buffer_size = (int) buffer_size;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "num_buffers") == 0) {
                long num_buffers = 0;
                if (ei_decode_long(buf, index, &num_buffers) == 0) {
                    connection_options->num_buffers = (int) num_buffers;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "dest_host") == 0) {
                if (ei_decode_string(buf, index, connection_options->dest_host) != 0) {
                    return false;
                }
            } else if (strcmp(atom, "dest_port") == 0) {
                if (ei_decode_string(buf, index, connection_options->dest_port) != 0) {
                    return false;
                }
            } else if (strcmp(atom, "timeout") == 0) {
                long timeout = 0;
                if (ei_decode_long(buf, index, &timeout) == 0) {
                    connection_options->timeout = (int) timeout;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

static bool rdma_drv_connection_options_parse(RdmaDrvConnectionOptions *connection_options, char *buf) {
    int index = 0;
    int version = 0;
    int arity = 0;

    if (ei_decode_version(buf, &index, &version) != 0) {
        return false;
    }

    if (ei_decode_list_header(buf, &index, &arity) == 0) {
        int i;

        for (i = 0; i < arity; i++) {
            int type = 0;
            int size = 0;

            if (ei_get_type(buf, &index, &type, &size) == 0) {
                switch (type) {
                    case ERL_ATOM_EXT:
                        if (!rdma_drv_connection_options_parse_atom(connection_options, buf, &index)) {
                            return false;
                        }
                        break;

                    case ERL_SMALL_TUPLE_EXT:
                        if (!rdma_drv_connection_options_parse_tuple(connection_options, buf, &index)) {
                            return false;
                        }
                        break;

                    default:
                        return false;
                }
            } else {
                return false;
            }
        }
    } else {
        return false;
    }

    return true;
}

static void rdma_drv_buffers_init(RdmaDrvBuffers *buffers, int buffer_size, int num_buffers) {
    buffers->buffer_size = buffer_size;
    buffers->num_buffers = num_buffers;
    buffers->buffers = driver_alloc(num_buffers * buffer_size);
    buffers->i = 0;
    buffers->free_i = 0;
    buffers->busy_i = -1;
}

static void * rdma_drv_buffers_reserve_buffer(RdmaDrvBuffers *buffers) {
    if (buffers->free_i == buffers->busy_i) {
        return NULL;
    }

    if (buffers->busy_i == -1) {
        buffers->busy_i = buffers->free_i;
    }

    void *buffer = buffers->buffers + buffers->free_i * buffers->buffer_size;
    buffers->free_i = (buffers->free_i + 1) % buffers->num_buffers;

    return buffer;
}

static void rdma_drv_buffers_free_buffer(RdmaDrvBuffers *buffers) {
    if (++buffers->busy_i == buffers->free_i) {
        buffers->busy_i = -1;
    }
}

static void * rdma_drv_buffers_current_buffer(RdmaDrvBuffers *buffers) {
    void *buffer = buffers->buffers + buffers->i * buffers->buffer_size;
    buffers->i = (buffers->i + 1) % buffers->num_buffers;

    return buffer;
}

static inline void start_timer(RdmaDrvData *data) {
    gettimeofday(&data->time_start, NULL);
}

static inline void stop_timer(RdmaDrvData *data) {
    struct timeval now;
    gettimeofday(&now, NULL);
    data->op_time = now.tv_usec - data->time_start.tv_usec;
}

static void rdma_drv_post_ack(RdmaDrvData *data) {
    if (data->sending_ack || data->pending_acks == 0) {
        return;
    }

    struct ibv_send_wr wr = {}, *bad_wr = NULL;

    wr.wr_id = ACK;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(ACK | data->pending_acks);

    ibv_post_send(data->id->qp, &wr, &bad_wr);

    data->sending_ack = true;
    data->pending_acks = 0;
}

static void rdma_drv_post_recv(RdmaDrvData *data, void *buffer) {
    struct ibv_recv_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) buffer;
    sge.length = data->connection_options.buffer_size;
    sge.lkey = data->recv_mr->lkey;

    ibv_post_recv(data->id->qp, &wr, &bad_wr);
}

static void rdma_drv_post_send(RdmaDrvData *data, void *buffer, ErlDrvSizeT remaining) {
    struct ibv_send_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(remaining);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) buffer;
    sge.length = remaining < data->connection_options.buffer_size ? remaining : data->connection_options.buffer_size;
    sge.lkey = data->send_mr->lkey;

    ibv_post_send(data->id->qp, &wr, &bad_wr);

    data->peer_ready--;
}

static void rdma_drv_init_ibverbs(RdmaDrvData *data) {
    data->pd = ibv_alloc_pd(data->id->verbs);

    data->comp_channel = ibv_create_comp_channel(data->id->verbs);
    fcntl(data->comp_channel->fd, F_SETFL, fcntl(data->comp_channel->fd, F_GETFL) | O_NONBLOCK);
    driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 1);

    data->cq = ibv_create_cq(data->id->verbs, data->connection_options.num_buffers * 2, NULL, data->comp_channel, 0);
    ibv_req_notify_cq(data->cq, 0);

    rdma_drv_buffers_init(&data->send_buffers, data->connection_options.buffer_size, data->connection_options.num_buffers - 1);
    rdma_drv_buffers_init(&data->recv_buffers, data->connection_options.buffer_size, data->connection_options.num_buffers);

    data->send_mr = ibv_reg_mr(data->pd, data->send_buffers.buffers, data->connection_options.buffer_size * (data->connection_options.num_buffers - 1), IBV_ACCESS_LOCAL_WRITE);
    data->recv_mr = ibv_reg_mr(data->pd, data->recv_buffers.buffers, data->connection_options.buffer_size * data->connection_options.num_buffers, IBV_ACCESS_LOCAL_WRITE);

    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.send_cq = data->cq;
    qp_attr.recv_cq = data->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = data->connection_options.num_buffers;
    qp_attr.cap.max_recv_wr = data->connection_options.num_buffers;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    rdma_create_qp(data->id, data->pd, &qp_attr);

    void *recv_buffer;
    while ((recv_buffer = rdma_drv_buffers_reserve_buffer(&data->recv_buffers))) {
        nsrdma_drv_post_recv(data, recv_buffer);
    }

    data->peer_ready = data->connection_options.num_buffers - 1;    // one receieve buffer is reserved for ACK
}

static ErlDrvData rdma_drv_start(ErlDrvPort port, char *command) {
    RdmaDrvData *data = (RdmaDrvData *) driver_alloc(sizeof(RdmaDrvData));
    memset(data, 0, sizeof(RdmaDrvData));
    data->port = port;

    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData) data;
}

static void rdma_drv_stop(ErlDrvData drv_data) {
    // XXX: Rememeber to flush queue or driver won't close

    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    if (data->comp_channel) {
        driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 0);
    }

    if (data->id) {
        rdma_destroy_id(data->id);
    }

    if (data->ec) {
        driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 0);
        rdma_destroy_event_channel(data->ec);
    }

    driver_free(data);
}

static ErlDrvSizeT rdma_drv_send_fully(RdmaDrvData *data, void *buf, ErlDrvSizeT len, bool buf_is_vec) {
    void *send_buffer;
    ErlDrvSizeT remaining = len;

    while (data->peer_ready && (send_buffer = rdma_drv_buffers_reserve_buffer(&data->send_buffers))) {
        ErlDrvSizeT send_amount = remaining < data->connection_options.buffer_size ? remaining : data->connection_options.buffer_size;

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
        }
    }

    return true;
}

static void rdma_drv_output(ErlDrvData drv_data, char *buf, ErlDrvSizeT len) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    start_timer(data);

    if (rdma_drv_flush_queue(data)) {
        ErlDrvSizeT remaining = rdma_drv_send_fully(data, buf, len, false);
        if (remaining > 0) {
            driver_enq(data->port, (char *) &remaining, sizeof(remaining));
            driver_enq(data->port, buf + (len - remaining), remaining);
        }
    } else {
        driver_enq(data->port, (char *) &len, sizeof(len));
        driver_enq(data->port, buf, len);
    }

    stop_timer(data);
}

static void rdma_drv_handle_rdma_cm_event_connect_request(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    ErlDrvPort new_port;
    RdmaDrvData *new_data;
    struct rdma_conn_param cm_params = {};

    new_data = (RdmaDrvData *) driver_alloc(sizeof(RdmaDrvData));
    memset(new_data, 0, sizeof(RdmaDrvData));
    new_port = driver_create_port(data->port, driver_connected(data->port), "rdma_drv", (ErlDrvData) new_data);
    set_port_control_flags(new_port, PORT_CONTROL_FLAG_BINARY);

    new_data->id = cm_event->id;
    new_data->id->context = new_data;
    new_data->port = new_port;
    new_data->connection_options = data->connection_options;

    rdma_drv_init_ibverbs(new_data);
    rdma_accept(new_data->id, &cm_params);
}

static void rdma_drv_handle_rdma_cm_event_addr_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    rdma_resolve_route(data->id, data->connection_options.timeout);
}

static void rdma_drv_handle_rdma_cm_event_route_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    struct rdma_conn_param cm_params = {};
    rdma_drv_init_ibverbs(data);
    rdma_connect(data->id, &cm_params);
}

static void rdma_drv_handle_rdma_cm_event_established(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    if (cm_event->id->context) {
        RdmaDrvData *new_data = (RdmaDrvData *) cm_event->id->context;

        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_ATOM, driver_mk_atom("accept"),
            ERL_DRV_PORT, driver_mk_port(new_data->port),
            ERL_DRV_TUPLE, 3,
        };

        erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
    } else {
        ErlDrvTermData spec[] = {
            ERL_DRV_PORT, driver_mk_port(data->port),
            ERL_DRV_ATOM, driver_mk_atom("established"),
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));

        data->state = CONNECTED;
    }
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
            rdma_drv_send_error_atom(data, "closed");
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

static void rdma_drv_handle_recv_complete(RdmaDrvData *data, struct ibv_wc *wc) {
    uint32_t imm_data = ntohl(wc->imm_data);
    void *recv_buffer = rdma_drv_buffers_current_buffer(&data->recv_buffers);

    if (imm_data & ACK) {
        data->peer_ready += imm_data & ~ACK;
        rdma_drv_post_recv(data, recv_buffer);
        rdma_drv_flush_queue(data);
    } else {
        ErlDrvSizeT remaining = imm_data;
        ErlDrvSizeT recv_amount = remaining < data->connection_options.buffer_size ? remaining : data->connection_options.buffer_size;

        if (remaining > data->connection_options.buffer_size && !data->incomplete_recv) {
            data->incomplete_recv = driver_alloc(remaining);
            data->incomplete_recv_offset = 0;
        }

        if (data->incomplete_recv) {
            memcpy(data->incomplete_recv + data->incomplete_recv_offset, recv_buffer, recv_amount);
            data->incomplete_recv_offset += recv_amount;
        }

        if (remaining <= data->connection_options.buffer_size) {
            if (data->incomplete_recv) {
                ErlDrvTermData spec[] =  {
                    ERL_DRV_PORT, driver_mk_port(data->port),
                    ERL_DRV_ATOM, driver_mk_atom("data"),
                    data->connection_options.binary ? ERL_DRV_BUF2BINARY : ERL_DRV_STRING, (ErlDrvTermData) data->incomplete_recv, data->incomplete_recv_offset,
                    ERL_DRV_TUPLE, 3,
                }; 

                erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));

                driver_free(data->incomplete_recv);
                data->incomplete_recv = NULL;
                data->incomplete_recv_offset = 0;
            } else {
                ErlDrvTermData spec[] =  {
                    ERL_DRV_PORT, driver_mk_port(data->port),
                    ERL_DRV_ATOM, driver_mk_atom("data"),
                    data->connection_options.binary ? ERL_DRV_BUF2BINARY : ERL_DRV_STRING, (ErlDrvTermData) recv_buffer, remaining,
                    ERL_DRV_TUPLE, 3,
                };   

                erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
            }
        }

        rdma_drv_post_recv(data, recv_buffer);
        data->pending_acks++;
    }
}

static void rdma_drv_handle_send_complete(RdmaDrvData *data, struct ibv_wc *wc) {
    if (wc->wr_id == ACK) {
        data->sending_ack = false;
    } else {
        rdma_drv_buffers_free_buffer(&data->send_buffers);
        rdma_drv_flush_queue(data);
    }
}

static void rdma_drv_handle_comp_channel_event(RdmaDrvData *data) {
    void *ev_ctx;
    struct ibv_cq *cq;
    struct ibv_wc wc;

    ibv_get_cq_event(data->comp_channel, &cq, &ev_ctx);
    ibv_ack_cq_events(cq, 1);
    ibv_req_notify_cq(cq, 0);

    while (ibv_poll_cq(cq, 1, &wc)) {
        if (wc.opcode & IBV_WC_RECV) {
            rdma_drv_handle_recv_complete(data, &wc);
        } else if (wc.opcode == IBV_WC_SEND) {
            rdma_drv_handle_send_complete(data, &wc);
        }
    }

    rdma_drv_post_ack(data);
}

static void rdma_drv_ready_input(ErlDrvData drv_data, ErlDrvEvent event) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    if (data->ec && event == (ErlDrvEvent) data->ec->fd) {
        rdma_drv_handle_rdma_cm_event(data);
    } else if (data->comp_channel && event == (ErlDrvEvent) data->comp_channel->fd) {
        start_timer(data);
        rdma_drv_handle_comp_channel_event(data);
        stop_timer(data);
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
    rdma_drv_connection_options_init(&data->connection_options);
    if (!rdma_drv_connection_options_parse(&data->connection_options, buf)) {
        rdma_drv_encode_error_atom(x, "bad_options");
        return;
    }

    /* Resolve the address and port */
    struct addrinfo *addr;
    struct addrinfo *hints = NULL;
    ret = getaddrinfo(data->connection_options.dest_host, data->connection_options.dest_port, hints, &addr);
    if (ret == EAI_SYSTEM) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    } else if (ret) {
        rdma_drv_encode_error_string(x, gai_strerror(ret));
        return;
    }

    /* Create the event channel */
    data->ec = rdma_create_event_channel();
    if (!data->ec) {
        rdma_drv_encode_error_string(x, "rdma_create_event_channel");
        return;
    }

    /*
     * Make event channel non-blocking and tell the driver to poll the
     * channel.
     */
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);
    driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 1);

    /* Create the "socket" */
    ret = rdma_create_id(data->ec, &data->id, NULL, RDMA_PS_TCP);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Start address resolution */
    struct sockaddr_in src_addr = {};
    src_addr.sin_family = AF_INET;
    src_addr.sin_port = htons(data->connection_options.port);
    src_addr.sin_addr = data->connection_options.ip;
    ret = rdma_resolve_addr(data->id, (struct sockaddr *) &src_addr, addr->ai_addr, data->connection_options.timeout);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    freeaddrinfo(addr);

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_listen(RdmaDrvData *data, char *buf, ei_x_buff *x) {
    int ret;

    /* Parse the connection options */
    rdma_drv_connection_options_init(&data->connection_options);
    if (!rdma_drv_connection_options_parse(&data->connection_options, buf)) {
        rdma_drv_encode_error_atom(x, "bad_options");
        return;
    }

    /* Create the event channel */
    data->ec = rdma_create_event_channel();
    if (!data->ec) {
        rdma_drv_encode_error_string(x, "rdma_create_event_channel");
        return;
    }  

    /*
     * Make event channel non-blocking and tell the driver to poll the
     * channel.
     */
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);
    driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 1);

    /* Create the "socket" */
    ret = rdma_create_id(data->ec, &data->id, NULL, RDMA_PS_TCP);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Bind to a specified port */
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(data->connection_options.port);
    addr.sin_addr = data->connection_options.ip;
    ret = rdma_bind_addr(data->id, (struct sockaddr *) &addr);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    /* Start listening for incoming connections */
    ret = rdma_listen(data->id, data->connection_options.backlog);
    if (ret) {
        rdma_drv_encode_error_posix(x, errno);
        return;
    }

    data->state = LISTENING;

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_port(RdmaDrvData *data, ei_x_buff *x) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_ulong(x, ntohs(rdma_get_src_port(data->id)));
}

static void rdma_drv_control_time(RdmaDrvData *data, ei_x_buff *x) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_ulong(x, data->op_time);
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

        case DRV_PORT:
            rdma_drv_control_port(data, &x);
            break;

        case DRV_TIME:
            rdma_drv_control_time(data, &x);
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
