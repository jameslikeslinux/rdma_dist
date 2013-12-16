/*
 * rdma_drv_options.h
 * Copyright (C) 2013 James Lee
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
