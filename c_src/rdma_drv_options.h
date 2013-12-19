/*
 * rdma_drv_options.h
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

#ifndef RDMA_DRV_OPTIONS_H
#define RDMA_DRV_OPTIONS_H

#include <stdbool.h>

typedef struct {
    bool binary;
    bool active;
    int backlog;
    short port;
    char ip[INET6_ADDRSTRLEN];
    int buffer_size;
    int num_buffers;
    char dest_host[NI_MAXHOST];
    char dest_port[NI_MAXSERV];
    int timeout;
} RdmaDrvOptions;

void rdma_drv_options_init(RdmaDrvOptions *options);
bool rdma_drv_options_parse(RdmaDrvOptions *options, char *buf);

#endif /* RDMA_DRV_OPTIONS_H */
