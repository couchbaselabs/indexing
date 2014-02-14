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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "rle.h"

/* for compression, use inline compression as much as possible
 * with minimal required additional buffer space, then release
 * unused memory space after compression
 */

/* traditional run length encoding algorithm that is suited for
 * in memory compression
 */
rle_encode_error_t rle_enc_trd(sized_buf *in,
                               sized_buf **out) {
    int32_t len;
    rle_encode_error_t errcode = RLE_ENCODE_SUCCESS;

    len = in->size;
    if ((len <= 2)) {
        return errcode;
    }
    //to be finished

    return errcode;
}

rle_decode_error_t rle_dec_trd(sized_buf *in,
                               sized_buf **out){
    int32_t len;
    rle_decode_error_t errcode = RLE_DECODE_SUCCESS;

    len = in->size;
    if ((len <= 2)) {
        return errcode;
    }
    //to be finished

    return errcode;
}


/* run length encoding based on PackBits algorithm that is suited for
 * in memory compression
 */
rle_encode_error_t rle_enc_pkb(sized_buf *in,
                               sized_buf **out){
    int32_t len;
    rle_encode_error_t errcode = RLE_ENCODE_SUCCESS;

    len = in->size;
    if ((len <= 2)) {
        return errcode;
    }
    //to be finished

    return errcode;
}

rle_decode_error_t rle_dec_pkb(sized_buf *in,
                               sized_buf **out) {
    int32_t len;
    rle_decode_error_t errcode = RLE_DECODE_SUCCESS;

    len = in->size;
    if ((len <= 2)) {
        return errcode;
    }
    //to be finished

    return errcode;
}

