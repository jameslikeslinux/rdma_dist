/*
 * rdma_drv_buffers.h
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

#ifndef RDMA_DRV_BUFFERS_H
#define RDMA_DRV_BUFFERS_H

#include <stdbool.h>

typedef struct {
    void *buffers;
    int buffer_size;
    int num_buffers;
    int i;
    int free_i;
    int busy_i;
} RdmaDrvBuffers;

bool rdma_drv_buffers_init(RdmaDrvBuffers *buffers, int buffer_size, int num_buffers);
void rdma_drv_buffers_free(RdmaDrvBuffers *buffers);
void * rdma_drv_buffers_reserve_buffer(RdmaDrvBuffers *buffers);
void rdma_drv_buffers_release_buffer(RdmaDrvBuffers *buffers);
void * rdma_drv_buffers_current_buffer(RdmaDrvBuffers *buffers);

#endif /* RDMA_DRV_BUFFERS_H */
