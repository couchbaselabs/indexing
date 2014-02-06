/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Fulu Li  <fulu@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#ifndef _DELTA_H
#define _DELTA_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
    typedef struct {
        char *buf;
        uint8_t size;
        uint8_t new_size;
    } sized_buf;

    typedef enum {
        DELTA_ENCODE_SUCCESS,
        DELTA_ENCODE_ERROR_INPUT_INVALID,
        DELTA_ENCODE_ERROR_ALLOCATION_FAILURE
    } delta_encode_error_t;

    typedef enum {
        DELTA_DECODE_SUCCESS,
        DELTA_DECODE_ERROR_INPUT_INVALID,
        DELTA_DECODE_ERROR_ALLOCATION_FAILURE
    } delta_decode_error_t;

    delta_encode_error_t delta_encode(sized_buf *arr,
                                      uint16_t len,
                                      uint16_t **new_idx);

    delta_decode_error_t delta_decode(const sized_buf *arr,
                                      uint16_t len,
                                      uint16_t idx,
                                      char **buf);

#ifdef __cplusplus
}
#endif

#endif

