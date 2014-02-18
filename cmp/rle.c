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
#include <limits.h>
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
    uint32_t len, cmp_len;
    char *bytes = NULL;
    char buf[256], curr, prev;
    uint16_t count; //USHRT_MAX is the limit
    rle_encode_error_t errcode = RLE_ENCODE_SUCCESS;

    len = in->org_size;
    if ((len <= 2)) {
        //no cmp is needed
        in->cmp_size = len;
        *out = in;
        return errcode;
    }
    cmp_len = 0;
    count = 0;
    bytes = in->buf;
    prev = in->buf[0];
    cmp_len++;
    len--;
    in->buf++;
    curr = in->buf[0];
    cmp_len++;
    len--;
    while (len > 0) {
    //to be finished



    }


    //TBD: finalize the value of cmp_size

out:
    //TBD: free unused space
    in->buf = bytes;
    //realloc()

    *out = in;
    return errcode;
}

rle_decode_error_t rle_dec_trd(sized_buf *in,
                               sized_buf **out){
    uint32_t len, cmp_len;
    char *bytes = NULL;
    char buf[256], curr, prev;
    uint16_t count; //USHRT_MAX is the limit
    rle_decode_error_t errcode = RLE_DECODE_SUCCESS;

    len = in->orig_size;
    cmp_len = in->cmp_size;

    if ((len <= 2)) {
        //nothing is cmped
        *out = in;
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
    uint32_t len, cmp_len;
    char *bytes = NULL;
    char buf[256], curr, prev;
    uint16_t count; //USHRT_MAX is the limit
    rle_encode_error_t errcode = RLE_ENCODE_SUCCESS;

    len = in->orig_size;
    if ((len <= 2)) {
        //no cmp is needed
        in->cmp_size = len;
        *out = in;
        return errcode;
    }
    //to be finished

    return errcode;
}

rle_decode_error_t rle_dec_pkb(sized_buf *in,
                               sized_buf **out) {
    uint32_t len, cmp_len;
    char *bytes = NULL;
    char buf[256], curr, prev;
    uint16_t count; //USHRT_MAX is the limit
    rle_decode_error_t errcode = RLE_DECODE_SUCCESS;

    len = in->orig_size;
    cmp_len = in->cmp_size;
    
    if ((len <= 2)) {
        //nothing is cmped
        *out = in;
        return errcode;
    }
    //to be finished

    return errcode;
}


