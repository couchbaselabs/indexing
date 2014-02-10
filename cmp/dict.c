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
#include <math.h>
#include "dict.h"

static void free_list_node(sym_list_t *node);
static sym_table_error_t insert_item(sym_table_t **st,
                                     sym_list_t *item);
static uint8_t actual_size(uint16_t num);

//similar to string comparison
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
 * init. the sym table 
 * 
 */

sym_table_error_t st_init(sym_table_t **st)
{
    sym_table_t *new_st = NULL;
    sym_table_error_t errcode = ST_SUCCESS;

    new_st = (sym_table_t *) calloc(1, sizeof(sym_table_t));
    if (new_st == NULL) {
        errcode = ST_ERROR_ALLOCATION_FAILURE;
        goto out;
    }

    *st = new_st;

out:
    return errcode;
}

/*
 * clean up the sym table, remove those symbols without a token
 * at the end due to the lower frequency of those symbols
 * This is triggered by (1) the ratio of num_of_total and 
 * num_of_tokens reaches a certain threshold, or (2) the num_of_total
 * reaches a certain limit 
 * 
 */
void st_cleanup(sym_table_t **st)
{
    sym_table_t *new_st = NULL;
    sym_list_t *lst_head = NULL;
    sym_list_t *tmp = NULL;
    sym_list_t *tmp2 = NULL;
    sym_list_t *pre = NULL;

    new_st = *st;
    lst_head = new_st->sym_list;

    tmp = lst_head;
    while((tmp != NULL) && (tmp->token == NULL)) {
        free_list_node(tmp);
        pre = tmp;
        tmp = tmp->next;
        free(pre);
    }
    lst_head = tmp;
    if (tmp != NULL) {
        pre = tmp;
        tmp = tmp->next;
        while (tmp != NULL){
            if (tmp->token == NULL) {
                pre->next = tmp->next;
                tmp2 = tmp;
                tmp = tmp->next;
                free_list_node(tmp2);
                free(tmp2);
            }else {
                pre = tmp;
                tmp = tmp->next;
            }
        }
    }
    new_st->sym_list = lst_head;
    *st = new_st;
}

/*
 * free some structures in a list node
 * 
 */
static void free_list_node(sym_list_t *node)
{
    if (node->symbol != NULL) {
        free(node->symbol->buf);
        free(node->symbol);
    }
    if (node->token != NULL) {
        free(node->token->buf);
        free(node->token);
    }
}

/*
 * given a symbol, returns its token
 * if not, returns (size 0, buf NULL) 
 * Note that the linklist is sorted ascendingly
 * NOTE that token starts from 1
 */
sized_buf st_lookup_s2t(sym_table_t *st,
                    sized_buf *sym)
{
    sym_list_t *tmp = NULL;
    sized_buf result;
    int rtn = 1; 

    result.size = 0;
    result.buf = NULL;

    tmp = st->sym_list;
    while (tmp != NULL){
        rtn = cmpfunc(sym, tmp->symbol);
        if (rtn == 0) {
            if (tmp->token != NULL) {
                result.size = tmp->token->size;
                result.buf = tmp->token->buf;
            }else {
                result.size = 255;
            }
            break;
        }else {
            if (rtn < 0) {
                break;
            }else {
                tmp = tmp->next;
            }
        }
    }

    return result;
}

/*
 * given a token, returns its symbol
 * if not, returns (size 0, buf NULL) 
 * Note that the linklist is sorted ascendingly
 * NOTE that token starts from 1 and 
 * current maximum of token value is
 * st->num_of_tokens
 */
sized_buf st_lookup_t2s(sym_table_t *st,
                    sized_buf *tok)
{
    sym_list_t *tmp = NULL;
    sized_buf result;
    int rtn = 1; 
    sized_buf curmax;

    result.size = 0;
    result.buf = NULL;

    curmax.size = actual_size(st->num_of_tokens);
    curmax.buf = (char *)malloc(curmax.size);
    if (curmax.buf == NULL) {
        goto out;
    }
    memcpy(curmax.buf, &st->num_of_tokens, curmax.size);
    if (cmpfunc(&curmax, tok) < 0) {
        goto out;
    }

    tmp = st->sym_list;
    while (tmp != NULL){
        if (tmp->token != NULL) {
            rtn = cmpfunc(tok, tmp->token);
        }
        if (rtn == 0) {
            result.size = tmp->symbol->size;
            result.buf = tmp->symbol->buf;
            break;
        }else {
            if (rtn < 0) {
                break;
            }else {
                tmp = tmp->next;
            }
        }
    }

out:
    if (curmax.buf != NULL) {
        free(curmax.buf);
    }
    return result;
}
/*
 * insert an item into the list,
 * note we have a limit for the number of items
 *
 */
static sym_table_error_t insert_item(sym_table_t **st,
                                     sym_list_t *item)
{
    sym_list_t *tmp = NULL;
    sym_list_t *pre = NULL;
    sym_table_error_t errcode = ST_SUCCESS;
    sym_table_t *new_st = NULL;
    int rtn = 0;
    int inserted = 0;

    new_st = *st;

    if (((new_st->num_of_total + 1) > NUM_OF_ITEMS_LIMIT) || 
         (((1.0 * new_st->num_of_tokens * RATIO_THRESHOLD) > 
          (new_st->num_of_total + 1)) && 
          new_st->num_of_tokens > CLEANUP_THRESHOLD)){
        errcode = ST_ERROR_NEEDS_CLEANUP;
        return errcode;
    }
    tmp = new_st->sym_list;
    rtn = cmpfunc(item->symbol, tmp->symbol);
    if (rtn < 0) {
        item->next = tmp;
        new_st->sym_list = item;
    }else {
        while (tmp->next != NULL){
            pre = tmp;
            tmp = tmp->next;
            rtn = cmpfunc(item->symbol, tmp->symbol);
            if (rtn < 0) {
                pre->next = item;
                item->next = tmp;
                inserted = 1;
                break;
            }
        }
        if (inserted == 0) {
            tmp->next = item;
        }        
    }

out:
    *st = new_st;

    return errcode;
}

/*
 * insert a symbol into the sorted list
 * (1) If the symbol is already in the list, increase its freq if the token
 * is not assigned to the symbol yet. After the change of the freq, if 
 * it is higher than a given threshold (later on make it adaptive, fixed
 * for now) and the number of symbols with a token is within the given
 * limit, then assign a token to the given symbol in the linklist.
 * if a token is already assigned to it, do nothing
 * (2) If the symbol is not in the list, insert it into the list accordingly
 * and set its freq as 1.
 * Note that the linklist is sorted ascendingly
 * Also note that we have a limit for the number of symbols with a token
 * Token value starts with 1
 * (3) Need to update num_of_sym and num_of_total, when the ratio reaches a 
 * certain threshold, call st_cleanup()
 * (4) If it is multi-threaded, we may consider to use locks as well
 */
sym_table_error_t st_insert(sym_table_t **st,
                            sized_buf *sym)
{
    sym_table_t *new_st = NULL;
    sym_list_t *tmp = NULL;
    sym_list_t *tmp2 = NULL;
    sym_list_t *pre = NULL;
    int rtn = 0;
    int i; 
    uint16_t value; 
    sized_buf result;
    sym_table_error_t errcode = ST_SUCCESS;

    new_st = *st;
    tmp = new_st->sym_list;

    if (tmp == NULL) {
        tmp = (sym_list_t *) calloc(1, sizeof(sym_list_t));
        if (tmp == NULL) {
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        tmp->symbol = (sized_buf *) calloc(1, sizeof(sized_buf));
        if (tmp->symbol == NULL) {
            free(tmp);
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        tmp->symbol->size = sym->size;
        tmp->symbol->buf = (char *) malloc(sym->size);
        if (tmp->symbol->buf == NULL) {
            free(tmp->symbol);
            free(tmp);
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        memcpy(tmp->symbol->buf, sym->buf, sym->size);
        tmp->sfreq = 1;
        tmp->token = NULL;
        tmp->next = NULL;
        new_st->num_of_total++;
        new_st->sym_list = tmp;
        goto out;        
    }
    result = st_lookup_s2t(new_st, sym);
    if (result.size == 0) {
        tmp2 = (sym_list_t *) calloc(1, sizeof(sym_list_t));
        if (tmp2 == NULL) {
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        tmp2->symbol = (sized_buf *) calloc(1, sizeof(sized_buf));
        if (tmp2->symbol == NULL) {
            free(tmp2);
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        tmp2->symbol->size = sym->size;
        tmp2->symbol->buf = (char *) malloc(sym->size);
        if (tmp2->symbol->buf == NULL) {
            free(tmp2->symbol);
            free(tmp2);
            errcode = ST_ERROR_ALLOCATION_FAILURE;
            goto out;
        }
        memcpy(tmp2->symbol->buf, sym->buf, sym->size);
        tmp2->sfreq = 1;
        tmp2->token = NULL;
        tmp2->next = NULL;
        errcode = insert_item(&new_st, tmp2);
        if (errcode == ST_ERROR_NEEDS_CLEANUP){
            st_cleanup(&new_st);
            errcode = insert_item(&new_st, tmp2);
        }
        if (errcode == ST_SUCCESS) {
            new_st->num_of_total++;
        }
        goto out;        
    }else {
        while (tmp != NULL){
            rtn = cmpfunc(sym, tmp->symbol);
            if (rtn == 0) {
                if (tmp->token != NULL){
                    goto out;                    
                } else{
                    tmp->sfreq++;
                    if (tmp->sfreq >= FREQ_THRESHOLD){
                        new_st->num_of_tokens++;
                        if (new_st->num_of_tokens <= NUM_OF_TOKENS_LIMIT){
                            tmp->token = (sized_buf *)calloc(1, sizeof(sized_buf));
                            if (tmp->token == NULL) {
                                errcode = ST_ERROR_ALLOCATION_FAILURE;
                                goto out;
                            }
                            tmp->token->size = actual_size(new_st->num_of_tokens);
                            tmp->token->buf = (char *) malloc(tmp->token->size);
                            if (tmp->token->buf == NULL) {
                                free(tmp->token);
                                errcode = ST_ERROR_ALLOCATION_FAILURE;
                                goto out;
                            }
                            memcpy(tmp->token->buf, &new_st->num_of_tokens, tmp->token->size);
                        }
                    }
                    goto out;
                }
            }else {
                tmp = tmp->next;
            }
        }
    }

out:
    *st = new_st;

    return errcode;
}

//free space for the given symbol table
void st_free(sym_table_t *st)
{
    sym_list_t *tmp = NULL;
    sym_list_t *nxt = NULL;
    uint8_t i;

    tmp = st->sym_list;
    while(tmp != NULL) {
        nxt = tmp->next;
        if (tmp->symbol != NULL) {
            free(tmp->symbol->buf);
            free(tmp->symbol);
        }
        if (tmp->token != NULL) {
            free(tmp->token->buf);
            free(tmp->token);
        }
        free(tmp);
        tmp = nxt;
    }

    free(st);
}

//calculate the actual size of the token
static uint8_t actual_size(uint16_t num)
{
    uint8_t size = 0;

    if (num < pow(2, 8.0)){
        size = 1;
    } else {
        size = 2;
    }
    
    return size;
}

int main() {
//does nothing for now in the main
    return 0;
}
