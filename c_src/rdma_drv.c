#include <sys/time.h>
#include <stdbool.h>
#include <string.h>
#include <erl_driver.h>
#include <ei.h>
#include <rdma/rdma_cma.h>
#include <fcntl.h>

#define DRV_CONNECT       'C'
#define DRV_LISTEN        'L'
#define DRV_PORT          'P'
#define DRV_POLL          'p'
#define DRV_TIME          'T'

#define BUFFER_SIZE   1024
#define TIMEOUT_IN_MS 500

typedef struct {
    ErlDrvPort port;

    struct rdma_cm_id *id;
    struct rdma_event_channel *ec;

    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    struct ibv_mr *send_mr, *recv_mr;
    char *send_region, *recv_region;

    struct timeval time_start;
    int op_time;
} RdmaDrvData;

static inline void start_timer(RdmaDrvData *data) {
    gettimeofday(&data->time_start, NULL);
}

static inline void stop_timer(RdmaDrvData *data) {
    struct timeval now;
    gettimeofday(&now, NULL);
    data->op_time = now.tv_usec - data->time_start.tv_usec;
}

static void post_ready(RdmaDrvData *data) {
    printf("Sending READY\n");

    struct ibv_send_wr wr = {}, *bad_wr = NULL;

    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.imm_data = htonl(1);

    ibv_post_send(data->id->qp, &wr, &bad_wr);
}

static void post_receives(RdmaDrvData *data) {
    struct ibv_recv_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) data->recv_region;
    sge.length = BUFFER_SIZE;
    sge.lkey = data->recv_mr->lkey;

    ibv_post_recv(data->id->qp, &wr, &bad_wr);
}

static void post_send(RdmaDrvData *data) {
    struct ibv_send_wr wr = {}, *bad_wr = NULL;
    struct ibv_sge sge = {};

    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) data->send_region;
    sge.length = BUFFER_SIZE;
    sge.lkey = data->send_mr->lkey;

    ibv_post_send(data->id->qp, &wr, &bad_wr);
}

static void init_ibverbs(RdmaDrvData *data) {
    data->pd = ibv_alloc_pd(data->id->verbs);

    data->comp_channel = ibv_create_comp_channel(data->id->verbs);
    fcntl(data->comp_channel->fd, F_SETFL, fcntl(data->comp_channel->fd, F_GETFL) | O_NONBLOCK);

    data->cq = ibv_create_cq(data->id->verbs, 10, NULL, data->comp_channel, 0);
    ibv_req_notify_cq(data->cq, 0);

    data->send_region = (char *) driver_alloc(BUFFER_SIZE);
    data->recv_region = (char *) driver_alloc(BUFFER_SIZE);

    data->send_mr = ibv_reg_mr(data->pd, data->send_region, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
    data->recv_mr = ibv_reg_mr(data->pd, data->recv_region, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);

    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.send_cq = data->cq;
    qp_attr.recv_cq = data->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 10;
    qp_attr.cap.max_recv_wr = 10;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    rdma_create_qp(data->id, data->pd, &qp_attr);

    post_receives(data);
}

static ErlDrvBinary * ei_x_to_new_binary(ei_x_buff *x) {
    ErlDrvBinary *bin = driver_alloc_binary(x->index);

    if (bin != NULL)
        memcpy(&bin->orig_bytes[0], x->buff, x->index);

    return bin;
}

static ErlDrvData rdma_drv_start(ErlDrvPort port, char *command) {
    RdmaDrvData *data = (RdmaDrvData *) driver_alloc(sizeof(RdmaDrvData));
    memset(data, 0, sizeof(RdmaDrvData));
    data->port = port;

    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData) data;
}

static void rdma_drv_stop(ErlDrvData drv_data) {
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

static void rdma_drv_output(ErlDrvData drv_data, char *buf, ErlDrvSizeT len) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    start_timer(data);

    memcpy(data->send_region, buf, len < BUFFER_SIZE ? len : BUFFER_SIZE);
    post_send(data);

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

    init_ibverbs(new_data);
    rdma_accept(new_data->id, &cm_params);
}

static void rdma_drv_handle_rdma_cm_event_addr_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    rdma_resolve_route(data->id, TIMEOUT_IN_MS);
}

static void rdma_drv_handle_rdma_cm_event_route_resolved(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    init_ibverbs(data);
    driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 1);

    struct rdma_conn_param cm_params = {};
    rdma_connect(data->id, &cm_params);
}

static void rdma_drv_handle_rdma_cm_event_established(RdmaDrvData *data, struct rdma_cm_event *cm_event) {
    if (cm_event->id->context) {
        RdmaDrvData *new_data = (RdmaDrvData *) cm_event->id->context;

        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM, driver_mk_atom("event"),
                ERL_DRV_ATOM, driver_mk_atom("accept"),
                ERL_DRV_PORT, driver_mk_port(new_data->port),
                ERL_DRV_TUPLE, 2,
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
    } else {
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM, driver_mk_atom("event"),
            ERL_DRV_ATOM, driver_mk_atom("established"),
            ERL_DRV_TUPLE, 2,
        };

        erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
    }
}

static void rdma_drv_handle_rdma_cm_event(RdmaDrvData *data) {
    struct rdma_cm_event *cm_event;
    rdma_get_cm_event(data->ec, &cm_event);

    switch (cm_event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            rdma_drv_handle_rdma_cm_event_connect_request(data, cm_event);
            break;

        case RDMA_CM_EVENT_ADDR_RESOLVED:
            rdma_drv_handle_rdma_cm_event_addr_resolved(data, cm_event);
            break;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            rdma_drv_handle_rdma_cm_event_route_resolved(data, cm_event);
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            rdma_drv_handle_rdma_cm_event_established(data, cm_event);
            break;

        default:
            /* XXX: Do something */
            break;
    }

    rdma_ack_cm_event(cm_event);
}

static void rdma_drv_handle_comp_channel_event(RdmaDrvData *data) {
    void *ev_ctx;
    struct ibv_cq *cq;
    struct ibv_wc wc;

    ibv_get_cq_event(data->comp_channel, &cq, &ev_ctx);
    ibv_ack_cq_events(cq, 1);
    ibv_req_notify_cq(cq, 0);

    while (ibv_poll_cq(cq, 1, &wc)) {
        // XXX: check wc.status

        if (wc.opcode & IBV_WC_RECV) {
            ErlDrvTermData spec[] =  {
                ERL_DRV_ATOM, driver_mk_atom("event"),
                    ERL_DRV_ATOM, driver_mk_atom("recv"),
                    ERL_DRV_BUF2BINARY, (ErlDrvTermData) data->recv_region, BUFFER_SIZE,
                    ERL_DRV_TUPLE, 2,
                ERL_DRV_TUPLE, 2,
            };   

            erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));

            post_receives(data);
        } else if (wc.opcode == IBV_WC_SEND) {
            ErlDrvTermData spec[] =  {
                ERL_DRV_ATOM, driver_mk_atom("event"),
                ERL_DRV_ATOM, driver_mk_atom("sent"),
                ERL_DRV_TUPLE, 2,
            };   

            erl_drv_output_term(driver_mk_port(data->port), spec, sizeof(spec) / sizeof(spec[0]));
        }
    }
}

static void rdma_drv_ready_input(ErlDrvData drv_data, ErlDrvEvent event) {
    RdmaDrvData *data = (RdmaDrvData *) drv_data;

    if (data->ec && event == (ErlDrvEvent) data->ec->fd) {
        rdma_drv_handle_rdma_cm_event(data);
    } else if (data->comp_channel && event == (ErlDrvEvent) data->comp_channel->fd) {
        rdma_drv_handle_comp_channel_event(data);
    }
}

static void rdma_drv_control_connect(RdmaDrvData *data, char *buf, ei_x_buff *x) {
    char *colon = strchr(buf, ':');
    *colon = '\0';
    char *host = buf;
    char *port_number = colon + 1;

    // XXX: Check for errors
    struct addrinfo *addr;
    struct addrinfo *hints = NULL;
    getaddrinfo(host, port_number, hints, &addr);

    data->ec = rdma_create_event_channel();
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);
    driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 1);

    void *context = NULL;
    rdma_create_id(data->ec, &data->id, context, RDMA_PS_TCP);

    struct sockaddr *src_addr = NULL;
    rdma_resolve_addr(data->id, src_addr, addr->ai_addr, TIMEOUT_IN_MS);

    freeaddrinfo(addr);

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_listen(RdmaDrvData *data, ei_x_buff *x) {
    // XXX: Test
    data->ec = rdma_create_event_channel();

    // Make event channel non-blocking and poll the channel
    fcntl(data->ec->fd, F_SETFL, fcntl(data->ec->fd, F_GETFL) | O_NONBLOCK);
    driver_select(data->port, (ErlDrvEvent) data->ec->fd, ERL_DRV_READ, 1);

    // XXX: Test
    void *context = NULL;
    rdma_create_id(data->ec, &data->id, context, RDMA_PS_TCP);

    // XXX: Test
    struct sockaddr addr = {};
    rdma_bind_addr(data->id, &addr);

    // XXX: Test
    int backlog = 10;
    rdma_listen(data->id, backlog);

    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_port(RdmaDrvData *data, ei_x_buff *x) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_ulong(x, ntohs(rdma_get_src_port(data->id)));
}

static void rdma_drv_control_poll(RdmaDrvData *data, ei_x_buff *x) {
    driver_select(data->port, (ErlDrvEvent) data->comp_channel->fd, ERL_DRV_READ, 1);
    ei_x_encode_atom(x, "ok");
}

static void rdma_drv_control_time(RdmaDrvData *data, ei_x_buff *x) {
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "ok");
    ei_x_encode_ulong(x, data->op_time);
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
            rdma_drv_control_listen(data, &x);
            break;

        case DRV_PORT:
            rdma_drv_control_port(data, &x);
            break;

        case DRV_POLL:
            rdma_drv_control_poll(data, &x);
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
    ERL_DRV_FLAG_USE_PORT_LOCKING | ERL_DRV_FLAG_SOFT_BUSY,          /* Driver flags. Soft busy flag is required for distribution drivers */
    NULL,                            /* Reserved for internal use */
    NULL,                            /* process_exit callback */
    NULL                             /* stop_select callback */
};

DRIVER_INIT(rdma_drv)
{
    return &rdma_drv_entry;
}
