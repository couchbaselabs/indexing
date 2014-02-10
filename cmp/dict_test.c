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


int main()
{
    //to add more stuff here
    int n, i;
    sized_buf sym[20];
    sym_table_t *st;
    sym_table_error_t errcode = ST_SUCCESS;
    sized_buf tok;
    uint16_t tk;
    sized_buf rtn;

    for (i = 0; i < 20; ++i) {
        sym[i].buf = NULL; /* init. */
    }

    char bin0[] = {97,97,48,49};
    sym[0].buf = (char *)malloc(sizeof(bin0));
    if ((sym[0].buf) == NULL) {
        goto out;
    }
    sym[0].size = sizeof(bin0);
    memcpy(sym[0].buf, bin0, sym[0].size);
    
    char bin1[] = {97,97,48,49};
    sym[1].buf = (char *)malloc(sizeof(bin1));
    if ((sym[1].buf) == NULL) {
        goto out;
    }
    sym[1].size = sizeof(bin1);
    memcpy(sym[1].buf, bin1, sym[1].size);

    char bin2[] = {97,97,48,49};
    sym[2].buf = (char *)malloc(sizeof(bin2));
    if ((sym[2].buf) == NULL) {
        goto out;
    }
    sym[2].size = sizeof(bin2);
    memcpy(sym[2].buf, bin2, sym[2].size);

    char bin3[] = {97,97,48,49};
    sym[3].buf = (char *)malloc(sizeof(bin3));
    if ((sym[3].buf) == NULL) {
        goto out;
    }
    sym[3].size = sizeof(bin3);
    memcpy(sym[3].buf, bin3, sym[3].size);

    char bin4[] = {97,97,48,49};
    sym[4].buf = (char *)malloc(sizeof(bin4));
    if ((sym[4].buf) == NULL) {
        goto out;
    }
    sym[4].size = sizeof(bin4);
    memcpy(sym[4].buf, bin4, sym[4].size);

    char bin5[] = {97,97,48,48};
    sym[5].buf = (char *)malloc(sizeof(bin5));
    if ((sym[5].buf) == NULL) {
        goto out;
    }
    sym[5].size = sizeof(bin5);
    memcpy(sym[5].buf, bin5, sym[5].size);

    char bin6[] = {98,97,48,49,50,51};
    sym[6].buf = (char *)malloc(sizeof(bin6));
    if ((sym[6].buf) == NULL) {
        goto out;
    }
    sym[6].size = sizeof(bin6);
    memcpy(sym[6].buf, bin6, sym[6].size);

    char bin7[] = {98,97,48,49,50,51};
    sym[7].buf = (char *)malloc(sizeof(bin7));
    if ((sym[7].buf) == NULL) {
        goto out;
    }
    sym[7].size = sizeof(bin7);
    memcpy(sym[7].buf, bin7, sym[7].size);
    
    char bin8[] = {98,97,48,49,50,51};
    sym[8].buf = (char *)malloc(sizeof(bin8));
    if ((sym[8].buf) == NULL) {
        goto out;
    }
    sym[8].size = sizeof(bin8);
    memcpy(sym[8].buf, bin8, sym[8].size);
    
    char bin9[] = {98,97,48,49,50,51};
    sym[9].buf = (char *)malloc(sizeof(bin9));
    if ((sym[9].buf) == NULL) {
        goto out;
    }
    sym[9].size = sizeof(bin9);
    memcpy(sym[9].buf, bin9, sym[9].size);
    
    char bin10[] = {98,98,48,49,50,51,52};
    sym[10].buf = (char *)malloc(sizeof(bin10));
    if ((sym[10].buf) == NULL) {
        goto out;
    }
    sym[10].size = sizeof(bin10);
    memcpy(sym[10].buf, bin10, sym[10].size);
    
    char bin11[] = {98,98,48,49,50,51,52};
    sym[11].buf = (char *)malloc(sizeof(bin11));
    if ((sym[11].buf) == NULL) {
        goto out;
    }
    sym[11].size = sizeof(bin11);
    memcpy(sym[11].buf, bin11, sym[11].size);
    
    char bin12[] = {98,98,48,49,50,51,52};
    sym[12].buf = (char *)malloc(sizeof(bin12));
    if ((sym[12].buf) == NULL) {
        goto out;
    }
    sym[12].size = sizeof(bin12);
    memcpy(sym[12].buf, bin12, sym[12].size);
    
    char bin13[] = {98,98,48,49,50,51,52};
    sym[13].buf = (char *)malloc(sizeof(bin13));
    if ((sym[13].buf) == NULL) {
        goto out;
    }
    sym[13].size = sizeof(bin13);
    memcpy(sym[13].buf, bin13, sym[13].size);
    
    char bin14[] = {99,98,48,49,50,51,52,53};
    sym[14].buf = (char *)malloc(sizeof(bin14));
    if ((sym[14].buf) == NULL) {
        goto out;
    }
    sym[14].size = sizeof(bin14);
    memcpy(sym[14].buf, bin14, sym[14].size);
    
    char bin15[] = {99,98,48,49,50,51,52,53};
    sym[15].buf = (char *)malloc(sizeof(bin15));
    if ((sym[15].buf) == NULL) {
        goto out;
    }
    sym[15].size = sizeof(bin15);
    memcpy(sym[15].buf, bin15, sym[15].size);
    
    char bin16[] = {99,98,48,49,50,51,52,53};
    sym[16].buf = (char *)malloc(sizeof(bin16));
    if ((sym[16].buf) == NULL) {
        goto out;
    }
    sym[16].size = sizeof(bin16);
    memcpy(sym[16].buf, bin16, sym[16].size);
    
    char bin17[] = {99,98,48,49,50,51,52,53};
    sym[17].buf = (char *)malloc(sizeof(bin17));
    if ((sym[17].buf) == NULL) {
        goto out;
    }
    sym[17].size = sizeof(bin17);
    memcpy(sym[17].buf, bin17, sym[17].size);
    
    char bin18[] = {102,98,48,49,50,51};
    sym[18].buf = (char *)malloc(sizeof(bin18));
    if ((sym[18].buf) == NULL) {
        goto out;
    }
    sym[18].size = sizeof(bin18);
    memcpy(sym[18].buf, bin18, sym[18].size);
    
    char bin19[] = {101,98,48,49,50,52,53};
    sym[19].buf = (char *)malloc(sizeof(bin19));
    if ((sym[19].buf) == NULL) {
        goto out;
    }
    sym[19].size = sizeof(bin19);
    memcpy(sym[19].buf, bin19, sym[19].size);
    
    printf("Before sorting the list is: \n");
    for( n = 0 ; n < 20; n++ ) {
        for (i = 0; i < sym[n].size; ++i) {
            printf("%c", sym[n].buf[i]);
        }
        printf("\n");
    }

    //init
    printf("\n");
    printf("st init ... \n");
    errcode = st_init(&st);
    if (errcode != ST_SUCCESS) {
        goto out;        
    }

    for (i = 0; i < 20; ++i) {
        errcode = st_insert(&st, &sym[i]);
        if (errcode != ST_SUCCESS) {
            break;
        }
    }
    printf("\n");
    printf("st_insert... \n");
    printf("\n");
out:
    
    tok.size = 1;
    tk = 1; 
    tok.buf = (char *)malloc(tok.size);
    if (tok.buf == NULL) {
        goto out2;
    }
    memcpy(tok.buf, &tk, tok.size);
    for (i = 0; i < tok.size; ++i) {
        printf("lookup: token value is %d", tok.buf[i]);
    }
    printf("\n");
    rtn = st_lookup_t2s(st, &tok);
    printf("the returned symbol is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%c", rtn.buf[i]);
    }
    printf("\n");
    free(tok.buf);
    
    //st cleanup
    st_cleanup(&st);
        
    tok.size = 1;
    tk = 2; 
    tok.buf = (char *)malloc(tok.size);
    if (tok.buf == NULL) {
        goto out2;
    }
    memcpy(tok.buf, &tk, tok.size);
    rtn = st_lookup_t2s(st, &tok);
    for (i = 0; i < tok.size; ++i) {
        printf("Lookup: token value is %d", tok.buf[i]);
    }
    printf("\n");
    printf("the returned symbol is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%c", rtn.buf[i]);
    }
    printf("\n");
    //free memory
    free(tok.buf);
            
    tok.size = 1;
    tk = 3; 
    tok.buf = (char *)malloc(tok.size);
    if (tok.buf == NULL) {
        goto out2;
    }
    memcpy(tok.buf, &tk, tok.size);
    rtn = st_lookup_t2s(st, &tok);
    for (i = 0; i < tok.size; ++i) {
        printf("Lookup: token value is %d", tok.buf[i]);
    }
    printf("\n");
    printf("the returned symbol is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%c", rtn.buf[i]);
    }
    printf("\n");
    //free memory
    free(tok.buf);

    tok.size = 1;
    tk = 4; 
    tok.buf = (char *)malloc(tok.size);
    if (tok.buf == NULL) {
        goto out2;
    }
    memcpy(tok.buf, &tk, tok.size);
    rtn = st_lookup_t2s(st, &tok);
    for (i = 0; i < tok.size; ++i) {
        printf("Lookup: token value is %d", tok.buf[i]);
    }
    printf("\n");
    printf("the returned symbol is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%c", rtn.buf[i]);
    }
    printf("\n");
    //free memory
    free(tok.buf);

    tok.size = 1;
    tk = 5; 
    tok.buf = (char *)malloc(tok.size);
    if (tok.buf == NULL) {
        goto out2;
    }
    memcpy(tok.buf, &tk, tok.size);
    rtn = st_lookup_t2s(st, &tok);
    for (i = 0; i < tok.size; ++i) {
        printf("Lookup: token value is %d", tok.buf[i]);
    }
    printf("\n");
    if (rtn.size == 0) {
        printf("there is NO MATCH! \n");
    }else {
        printf("the returned symbol is: ");
        for (i = 0; i < rtn.size; ++i) {
            printf("%c", rtn.buf[i]);
        }
        printf("\n");
    }
    //free memory
    free(tok.buf);

    rtn = st_lookup_s2t(st, &sym[0]);
    printf("Lookup: symbol is: ");
    for (i = 0; i < sym[0].size; ++i) {
        printf("%c", sym[0].buf[i]);
    }
    printf("\n");
    printf("the returned token is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%d", rtn.buf[i]);
    }
    printf("\n");

    rtn = st_lookup_s2t(st, &sym[6]);
    printf("Lookup: symbol is: ");
    for (i = 0; i < sym[6].size; ++i) {
        printf("%c", sym[6].buf[i]);
    }
    printf("\n");
    printf("the returned token is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%d", rtn.buf[i]);
    }
    printf("\n");

    rtn = st_lookup_s2t(st, &sym[10]);
    printf("Lookup: symbol is: ");
    for (i = 0; i < sym[10].size; ++i) {
        printf("%c", sym[10].buf[i]);
    }
    printf("\n");
    printf("the returned token is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%d", rtn.buf[i]);
    }
    printf("\n");

    rtn = st_lookup_s2t(st, &sym[14]);
    printf("Lookup: symbol is: ");
    for (i = 0; i < sym[14].size; ++i) {
        printf("%c", sym[14].buf[i]);
    }
    printf("\n");
    printf("the returned token is: ");
    for (i = 0; i < rtn.size; ++i) {
        printf("%d", rtn.buf[i]);
    }
    printf("\n");

    rtn = st_lookup_s2t(st, &sym[5]);
    printf("Lookup: symbol is: ");
    for (i = 0; i < sym[5].size; ++i) {
        printf("%c", sym[5].buf[i]);
    }
    printf("\n");
    if (rtn.size == 0) {
        printf("there is NO MATCH !\n ");
    }else {
        printf("the returned token is: ");
        for (i = 0; i < rtn.size; ++i) {
            printf("%d", rtn.buf[i]);
        }
        printf("\n");
    }

out2:
    //free memory
    printf("st free memory ... \n");
    st_free(st);
    for (i = 0; i < 20; ++i) {
        if (sym[i].buf != NULL) {
            free(sym[i].buf);
        }
    }
    printf(" \n");
    return(0);
}
