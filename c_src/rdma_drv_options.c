/*
 * rdma_drv_options.c
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

#include <ei.h>
#include <string.h>

#include "rdma_drv_options.h"

void rdma_drv_options_init(RdmaDrvOptions *options) {
    options->binary = false;
    options->backlog = 5;
    options->port = 0;
    options->buffer_size = 1024;
    options->num_buffers = 100;
}

bool rdma_drv_options_parse_atom(RdmaDrvOptions *options, char *buf, int *index) {
    char atom[MAXATOMLEN];

    if (ei_decode_atom(buf, index, atom) == 0) {
        if (strcmp(atom, "list") == 0) {
            options->binary = false;
        } else if (strcmp(atom, "binary") == 0) {
            options->binary = true;
        } else {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

bool rdma_drv_options_parse_tuple(RdmaDrvOptions *options, char *buf, int *index) {
    int arity = 0;

    if (ei_decode_tuple_header(buf, index, &arity) == 0) {
        char atom[MAXATOMLEN];

        if (ei_decode_atom(buf, index, atom) == 0) {
            if (strcmp(atom, "backlog") == 0) {
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
                    case ERL_ATOM_EXT:
                        if (!rdma_drv_options_parse_atom(options, buf, &index)) {
                            return false;
                        }
                        break;

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
