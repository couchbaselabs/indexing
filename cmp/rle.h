/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @copyright 2014 Couchbase, Inc.
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

#ifndef _RLE_H
#define _RLE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
    typedef struct {
        char *buf;
        uint32_t size;
    } sized_buf;

    typedef enum {
        RLE_ENCODE_SUCCESS,
        RLE_ENCODE_ERROR_INPUT_INVALID,
        RLE_ENCODE_ERROR_ALLOCATION_FAILURE
    } rle_encode_error_t;

    typedef enum {
        RLE_DECODE_SUCCESS,
        RLE_DECODE_ERROR_INPUT_INVALID,
        RLE_DECODE_ERROR_ALLOCATION_FAILURE
    } rle_decode_error_t;

    /* traditional run length encoding algorithm that is suited for
       in memory compression
    */
    rle_encode_error_t rle_enc_trd(sized_buf *in,
                                   sized_buf **out);

    rle_decode_error_t rle_dec_trd(sized_buf *in,
                                   sized_buf **out);


    /* run length encoding based on PackBits algorithm that is suited for
       in memory compression
    */
    rle_encode_error_t rle_enc_pkb(sized_buf *in,
                                   sized_buf **out);

    rle_decode_error_t rle_dec_pkb(sized_buf *in,
                                   sized_buf **out);

#ifdef __cplusplus
}
#endif

#endif

