/*
 * rdma_drv_options.c
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

#include <ei.h>
#include <string.h>

#include "rdma_drv_options.h"

void rdma_drv_options_init(RdmaDrvOptions *options) {
    options->binary = false;
    options->active = false;
    options->packet = 0;
    options->backlog = 5;
    options->port = 0;
    options->buffer_size = 1024;
    options->num_buffers = 100;
}

static bool rdma_drv_options_parse_tuple(RdmaDrvOptions *options, char *buf, int *index) {
    int arity = 0;

    if (ei_decode_tuple_header(buf, index, &arity) == 0) {
        char atom[MAXATOMLEN];

        if (ei_decode_atom(buf, index, atom) == 0) {
            if (strcmp(atom, "list") == 0) {
                int list = 0;
                if (ei_decode_boolean(buf, index, &list) == 0) {
                    options->binary = !((bool) list);
                } else {
                    return false;
                }
            } else if (strcmp(atom, "binary") == 0) {
                int binary = 0;
                if (ei_decode_boolean(buf, index, &binary) == 0) {
                    options->binary = (bool) binary;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "active") == 0) {
                int active = 0;
                if (ei_decode_boolean(buf, index, &active) == 0) {
                    options->active = (bool) active;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "packet") == 0) {
                long packet = 0;
                if (ei_decode_long(buf, index, &packet) == 0) {
                    options->packet = (int) packet;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "backlog") == 0) {
                long backlog = 0;
                if (ei_decode_long(buf, index, &backlog) == 0) {
                    options->backlog = (int) backlog;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "port") == 0) {
                long port = 0;
                if (ei_decode_long(buf, index, &port) == 0) {
                    options->port = (short) port;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "ip") == 0) {
                if (ei_decode_string(buf, index, options->ip) != 0) {
                    return false;
                }
            } else if (strcmp(atom, "buffer_size") == 0) {
                long buffer_size = 0;
                if (ei_decode_long(buf, index, &buffer_size) == 0) {
                    options->buffer_size = (int) buffer_size;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "num_buffers") == 0) {
                long num_buffers = 0;
                if (ei_decode_long(buf, index, &num_buffers) == 0) {
                    options->num_buffers = (int) num_buffers;
                } else {
                    return false;
                }
            } else if (strcmp(atom, "dest_host") == 0) {
                if (ei_decode_string(buf, index, options->dest_host) != 0) {
                    return false;
                }
            } else if (strcmp(atom, "dest_port") == 0) {
                if (ei_decode_string(buf, index, options->dest_port) != 0) {
                    return false;
                }
            } else if (strcmp(atom, "timeout") == 0) {
                long timeout = 0;
                if (ei_decode_long(buf, index, &timeout) == 0) {
                    options->timeout = (int) timeout;
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

bool rdma_drv_options_parse(RdmaDrvOptions *options, char *buf) {
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
                    case ERL_SMALL_TUPLE_EXT:
                        if (!rdma_drv_options_parse_tuple(options, buf, &index)) {
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
