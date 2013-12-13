/*
 * rdma_drv_buffers.h
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
