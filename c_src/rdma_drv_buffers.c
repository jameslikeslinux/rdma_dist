/*
 * rdma_drv_buffers.c
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

#include <erl_driver.h>

#include "rdma_drv_buffers.h"

bool rdma_drv_buffers_init(RdmaDrvBuffers *buffers, int buffer_size, int num_buffers) {
    buffers->buffer_size = buffer_size;
    buffers->num_buffers = num_buffers;
    buffers->buffers = driver_alloc(num_buffers * buffer_size);
    buffers->i = 0;
    buffers->free_i = 0;
    buffers->busy_i = -1;

    return buffers->buffers != NULL;
}

void rdma_drv_buffers_free(RdmaDrvBuffers *buffers) {
    if (buffers->buffers) {
        driver_free(buffers->buffers);
        buffers->buffers = NULL;
    }
}

void * rdma_drv_buffers_reserve_buffer(RdmaDrvBuffers *buffers) {
    if (buffers->free_i == buffers->busy_i) {
        /* All buffers are busy. */
        return NULL;
    }

    if (buffers->busy_i == -1) {
        /*
         * No buffers were busy, but we're reserving one now, so set
         * the first busy buffer 
         */
        buffers->busy_i = buffers->free_i;
    }

    void *buffer = buffers->buffers + buffers->free_i * buffers->buffer_size;
    buffers->free_i = (buffers->free_i + 1) % buffers->num_buffers;

    return buffer;
}

void rdma_drv_buffers_release_buffer(RdmaDrvBuffers *buffers) {
    /*
     * Buffers are freed in the same order that they're reserved, so
     * all we have to do is mark the next buffer as the start of the
     * busy buffers.
     */

    if (++buffers->busy_i == buffers->free_i) {
        /* We've caught up to the first free buffer. */
        buffers->busy_i = -1;
    }
}

void * rdma_drv_buffers_current_buffer(RdmaDrvBuffers *buffers) {
    void *buffer = buffers->buffers + buffers->i * buffers->buffer_size;
    buffers->i = (buffers->i + 1) % buffers->num_buffers;

    return buffer;
}
