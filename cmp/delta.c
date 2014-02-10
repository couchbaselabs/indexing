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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "delta.h"

int cmpfunc(const void *e1, const void *e2)
{
    uint8_t size;
    sized_buf *b1, *b2;
    int result = 0;

    b1 = (sized_buf *) e1;
    b2 = (sized_buf *) e2;

    if (b2->size < b1->size) {
        size = b2->size;
    } else {
        size = b1->size;
    }

    result = memcmp(b1->buf, b2->buf, size);
    if (result == 0) {
        if (size < b2->size) {
            return -1;
        }
        else {
            if (size < b1->size) {
                return 1;
            }
        }
    }

    return result;
}

/*
 * Note if the return code is DELTA_ENCODE_SUCCESS, the caller need to free the new_idx 
 * after processing (setting the values to the original element structure, 
 * After setting new_idx for each metadata entry, free (new_idx);)
 *
 * For integration with ep engine, this arr should be declared in a commonly accessible place (global) 
 * by all storedItem instances. The placeholder can be two dimensional with the limit of each chunk is
 * less than 64K items. Following the way we are dealing with hashtable in ep engine
 * We can keep this array the same way as the hash table in ep-engine as a global variable
 * 
 */

delta_encode_error_t delta_encode(sized_buf *arr,
                                  uint16_t len,
                                  uint16_t **new_idx)
{
    uint16_t i, j, length, dlen, clen, gap;
    uint16_t *idx = NULL;
    sized_buf *bkp = NULL;
    uint16_t *mark = NULL;
    char *tmp = NULL;
    char *delta = NULL;
    delta_encode_error_t errcode = DELTA_ENCODE_SUCCESS;

    if ((len == 0) || (len == 1)) {
        return errcode;
    }
    
    bkp = (sized_buf *) calloc(len, sizeof(sized_buf));
    if (bkp == NULL) {
        errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
        goto out;
    }

    for (i = 0; i < len; ++i) {
        bkp[i].size = arr[i].size;
        bkp[i].buf = arr[i].buf;
    }    

    qsort(arr, len, sizeof(sized_buf), cmpfunc);

    mark = (uint16_t *) calloc(len, sizeof(uint16_t));
    if (mark == NULL) {
        errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
        goto out;
    }

    idx = (uint16_t *) calloc(len, sizeof(uint16_t));
    if (idx == NULL) {
        errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
        goto out;
    }

    for (i = 0; i < len; ++i){
        for (j = 0; j < len; ++j){
            if (mark[j] == 0) {
                if (memcmp(arr[j].buf, bkp[i].buf, bkp[i].size) == 0) {
                    idx[i] = j;
                    mark[j] = 1;            
                    break;
                }
            }
        }    
    }
    *new_idx = idx;

    arr[0].new_size = arr[0].size;

    delta = (char *) malloc(arr[0].size * sizeof(char));
    if (delta == NULL) {
        errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
        goto out;
    }
    memcpy(delta, arr[0].buf, arr[0].size);
    dlen = arr[0].size;

    for (i = 1; i < len; ++i) {
        tmp = (char *) malloc(arr[i].size * sizeof(char));
        if (tmp == NULL) {
            errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        memcpy(tmp, arr[i].buf, arr[i].size);
        clen = arr[i].size;
        length = dlen < clen ? dlen: clen;

        for (j = 0; j < length; ++j){
            arr[i].buf[j] ^= delta[j];
        }
        gap = 0;
        for (j = 0; j < length; ++j) {
            if (arr[i].buf[j] == 0) {
                gap++;
            }
            else {
                break;
            }
        }

        for (j = gap; j < clen; ++j) {
            arr[i].buf[j - gap] = arr[i].buf[j];
        }
        arr[i].buf = realloc(arr[i].buf, clen - gap);
        if (arr[i].buf == NULL) {
            errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
            free(tmp);
            goto out;
        }
        arr[i].new_size = clen - gap;
        dlen = clen;
        free(delta);
        delta = (char *) malloc(clen * sizeof(char));
        if (delta == NULL) {
            errcode = DELTA_ENCODE_ERROR_ALLOCATION_FAILURE;
            free(tmp);
            goto out;
        }
        memcpy(delta, tmp, clen);
        free(tmp);
    }

out:
    for (i = 0; i < len; ++i){
        bkp[i].buf = NULL; 
    }
    free(bkp);
    free(mark);
    if (errcode != DELTA_ENCODE_SUCCESS) {
        free(idx);
    }
    free(delta);
   
    return errcode;
}

/* Note: the decompressed key is in the buf, 
 * if the return code is DELTA_DECODE_SUCCESS, 
 * the caller needs to free buf after usage 
 * if the idx is not zero (not the first entry)
 * in the array */


delta_decode_error_t delta_decode(const sized_buf *arr,
                                  uint16_t len,
                                  uint16_t idx,
                                  char **buf) 
{
    uint16_t i, length, dlen, clen, gap;
    int j;
    delta_decode_error_t errcode = DELTA_DECODE_SUCCESS;
    char *tmp = NULL;
    char *delta = NULL;

    if ((len == 0) || (len == 1)) {
        return errcode;
    }
    if (idx > 0) {
        delta = (char *) malloc(arr[0].size * sizeof(char));
        if (delta == NULL) {
            errcode = DELTA_DECODE_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        memcpy(delta, arr[0].buf, arr[0].size);
    }

    for (i = 1; i <= idx; ++i){
        free(tmp);
        tmp = (char *) malloc(arr[i].size * sizeof(char));
        if (tmp == NULL) {
            errcode = DELTA_DECODE_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        dlen = arr[i - 1].size;
        clen = arr[i].size;
        length = clen < dlen? clen: dlen;
        gap = arr[i].size - arr[i].new_size;

        memcpy(tmp, arr[i].buf, arr[i].new_size);
        for (j = arr[i].new_size - 1; j >= 0; --j) {
            tmp[j + gap] = tmp[j];
        }
        for (j = 0; j < gap; ++j){
            tmp[j] = 0;
        }
        for (j = 0; j < length; ++j){
            tmp[j] ^= delta[j];
        }
        free(delta);
        delta = (char *) malloc(arr[i].size * sizeof(char));
        if (delta == NULL) {
            errcode = DELTA_DECODE_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        memcpy(delta, tmp, arr[i].size);
    }
  
    if (idx == 0) {
        *buf = arr[0].buf;
    }
    else {
        *buf = tmp;
    }

out:
    if (idx > 0) {
        free(delta);
    }
    if (errcode != DELTA_DECODE_SUCCESS) {
        free(tmp);
    }
    return errcode;
}

