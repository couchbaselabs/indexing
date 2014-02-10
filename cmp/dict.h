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

#ifndef _DICT_H
#define _DICT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

//note that token starts from 1
#define NUM_OF_TOKENS_LIMIT 65534
#define CLEANUP_THRESHOLD 30000
#define NUM_OF_ITEMS_LIMIT 1000000
#define RATIO_THRESHOLD 15
#define FREQ_THRESHOLD 4

#ifdef __cplusplus
extern "C" {
#endif

/* The logic is to call st_init(), st_insert(), then st_lookup(), then st_cleanup() is triggered 
 * when the ratio reaches a certain threshold (to be done in st_insert()),
 * finally, if all is done, call st_free()
 *
 * the way to use token,
 *
 * both the symbols and the tokens have variable sizes, that is why we use sized_buf
 * if the token size is 1, then replace the original symbol (seqno, item exp time, loc exp time)
 * with an uint8_t
 * if the token size is 2, then replace the original symbol with an uint16_t
 * how to release the memory (free the original node structure with a new smaller node structure)?
 *
 * the current design makes it easier to expand
 * 
 * link list and each node includes
 * original symbol and its corresponding token
 * symbol frequence, once it is given a token, the freq will not increase any more for now
 * initially, every symbol will be inserted into the linklist if it does not exist and 
 * increase its corresponding freq
 * only when the freq is greater than a givne threshold, it will be given a token
 * at the end of each round or triggered by some conditions, those that are not given 
 * a token will be removed from the linklist
 */

typedef struct {
    char *buf;
    uint8_t size;
} sized_buf;
    
typedef struct sym_list{
    sized_buf *symbol;
    uint8_t sfreq;
    sized_buf *token;
    struct sym_list *next;
} sym_list_t;

typedef struct {
    sym_list_t *sym_list;
    uint16_t num_of_tokens;
    uint32_t num_of_total;
} sym_table_t;

typedef enum {
    ST_SUCCESS,
    ST_ERROR_SYM_VALUE_INVALID,
    ST_ERROR_OUT_OF_TOKEN_LIMIT,
    ST_ERROR_NEEDS_CLEANUP,
    ST_ERROR_ALLOCATION_FAILURE
} sym_table_error_t;

sym_table_error_t st_init(sym_table_t **st);

sym_table_error_t st_insert(sym_table_t **st,
                            sized_buf *sym);

sized_buf st_lookup_s2t(sym_table_t *st,
                    sized_buf *sym);

sized_buf st_lookup_t2s(sym_table_t *st,
                    sized_buf *tok);

void st_cleanup(sym_table_t **st);

void st_free(sym_table_t *st);

#ifdef __cplusplus
}
#endif

#endif
