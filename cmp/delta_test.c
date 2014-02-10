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

int main()
{
    int n, i, j;
    sized_buf arr[20];
    uint16_t *new_idx, idx[20];
    char *b[20];

    for (i = 0; i < 20; ++i) {
        b[i] = NULL; /* init. */
    }

    for (i = 0; i < 20; ++i) {
        arr[i].buf = NULL; /* init. */
    }


    char bin0[] = {97,97,48,49,50,51};
    arr[0].buf = (char *)malloc(sizeof(bin0));
    if ((arr[0].buf) == NULL) {
        goto out;
    }
    arr[0].size = sizeof(bin0);
    memcpy(arr[0].buf, bin0, arr[0].size); 
    arr[0].new_size = 0;

    char bin1[] = {97,98,49,57};
    arr[1].buf = (char *)malloc(sizeof(bin1));
    if ((arr[1].buf) == NULL) {
        goto out;
    }
    arr[1].size = sizeof(bin1);
    memcpy(arr[1].buf, bin1, arr[1].size); 
    arr[1].new_size = 0;

    char bin2[] = {97,98,49,55,56};
    arr[2].buf = (char *)malloc(sizeof(bin2));
    if ((arr[2].buf) == NULL) {
        goto out;
    }
    arr[2].size = sizeof(bin2);
    memcpy(arr[2].buf, bin2, arr[2].size); 
    arr[2].new_size = 0;

    char bin3[] = {97,97,49,56};
    arr[3].buf = (char *)malloc(sizeof(bin3));
    if ((arr[3].buf) == NULL) {
        goto out;
    }
    arr[3].size = sizeof(bin3);
    memcpy(arr[3].buf, bin3, arr[3].size); 
    arr[3].new_size = 0;

    char bin4[] = {111,110,50,48};
    arr[4].buf = (char *)malloc(sizeof(bin4));
    if ((arr[4].buf) == NULL) {
        goto out;
    }
    arr[4].size = sizeof(bin4);
    memcpy(arr[4].buf, bin4, arr[4].size); 
    arr[4].new_size = 0;

    char bin5[] = {97,97,48,48,49,50,51,52};
    arr[5].buf = (char *)malloc(sizeof(bin5));
    if ((arr[5].buf) == NULL) {
        goto out;
    }
    arr[5].size = sizeof(bin5);
    memcpy(arr[5].buf, bin5, arr[5].size); 
    arr[5].new_size = 0;

    char bin6[] = {101,97,49,56,51};
    arr[6].buf = (char *)malloc(sizeof(bin6));
    if ((arr[6].buf) == NULL) {
        goto out;
    }
    arr[6].size = sizeof(bin6);
    memcpy(arr[6].buf, bin6, arr[6].size); 
    arr[6].new_size = 0;

    char bin7[] = {111,110,50,48,55};
    arr[7].buf = (char *)malloc(sizeof(bin7));
    if ((arr[7].buf) == NULL) {
        goto out;
    }
    arr[7].size = sizeof(bin7);
    memcpy(arr[7].buf, bin7, arr[7].size); 
    arr[7].new_size = 0;

    char bin8[] = {97,97,48,48,49,50,51,52};
    arr[8].buf = (char *)malloc(sizeof(bin8));
    if ((arr[8].buf) == NULL) {
        goto out;
    }
    arr[8].size = sizeof(bin8);
    memcpy(arr[8].buf, bin8, arr[8].size); 
    arr[8].new_size = 0;

    char bin9[] = {99,97,48,48,49,50,51};
    arr[9].buf = (char *)malloc(sizeof(bin9));
    if ((arr[9].buf) == NULL) {
        goto out;
    }
    arr[9].size = sizeof(bin9);
    memcpy(arr[9].buf, bin9, arr[9].size); 
    arr[9].new_size = 0;

    char bin10[] = {99,97,48,48,49,50,51,52};
    arr[10].buf = (char *)malloc(sizeof(bin10));
    if ((arr[10].buf) == NULL) {
        goto out;
    }
    arr[10].size = sizeof(bin10);
    memcpy(arr[10].buf, bin10, arr[10].size); 
    arr[10].new_size = 0;
    
    char bin11[] = {99,97,48,48,49,50,51};
    arr[11].buf = (char *)malloc(sizeof(bin11));
    if ((arr[11].buf) == NULL) {
        goto out;
    }
    arr[11].size = sizeof(bin11);
    memcpy(arr[11].buf, bin11, arr[11].size); 
    arr[11].new_size = 0;

    char bin12[] = {99,97,48,48,49,50,52};
    arr[12].buf = (char *)malloc(sizeof(bin12));
    if ((arr[12].buf) == NULL) {
        goto out;
    }
    arr[12].size = sizeof(bin12);
    memcpy(arr[12].buf, bin12, arr[12].size); 
    arr[12].new_size = 0;
    
    char bin13[] = {100,97,48,48,49,50,51};
    arr[13].buf = (char *)malloc(sizeof(bin13));
    if ((arr[13].buf) == NULL) {
        goto out;
    }
    arr[13].size = sizeof(bin13);
    memcpy(arr[13].buf, bin13, arr[13].size); 
    arr[13].new_size = 0;

    char bin14[] = {100,97,48,48,49,50,52};
    arr[14].buf = (char *)malloc(sizeof(bin14));
    if ((arr[14].buf) == NULL) {
        goto out;
    }
    arr[14].size = sizeof(bin14);
    memcpy(arr[14].buf, bin14, arr[14].size); 
    arr[14].new_size = 0;
    
    char bin15[] = {100,97,48,48,49};
    arr[15].buf = (char *)malloc(sizeof(bin15));
    if ((arr[15].buf) == NULL) {
        goto out;
    }
    arr[15].size = sizeof(bin15);
    memcpy(arr[15].buf, bin15, arr[15].size); 
    arr[15].new_size = 0;

    char bin16[] = {100,97,48,48,49,50,52};
    arr[16].buf = (char *)malloc(sizeof(bin16));
    if ((arr[16].buf) == NULL) {
        goto out;
    }
    arr[16].size = sizeof(bin16);
    memcpy(arr[16].buf, bin16, arr[16].size); 
    arr[16].new_size = 0;
    
    char bin17[] = {101,97,48,48,49,50,52};
    arr[17].buf = (char *)malloc(sizeof(bin17));
    if ((arr[17].buf) == NULL) {
        goto out;
    }
    arr[17].size = sizeof(bin17);
    memcpy(arr[17].buf, bin17, arr[17].size); 
    arr[17].new_size = 0;
    
    char bin18[] = {100,97,48,48,49};
    arr[18].buf = (char *)malloc(sizeof(bin18));
    if ((arr[18].buf) == NULL) {
        goto out;
    }
    arr[18].size = sizeof(bin18);
    memcpy(arr[18].buf, bin18, arr[18].size); 
    arr[18].new_size = 0;

    char bin19[] = {100,97,48,48,49,50,52};
    arr[19].buf = (char *)malloc(sizeof(bin19));
    if ((arr[19].buf) == NULL) {
        goto out;
    }
    arr[19].size = sizeof(bin19);
    memcpy(arr[19].buf, bin19, arr[19].size); 
    arr[19].new_size = 0;
    
    for( n = 0 ; n < 20; n++ ) {
        for (i = 0; i < arr[n].size; ++i) {
            printf("%c", arr[n].buf[i]);
        }
        printf("\n");
    }

    if (delta_encode(arr, 20, &new_idx) == DELTA_ENCODE_SUCCESS) {
        printf("delta encode succeeds!\n");
        printf("... \n");
    }

    for (i = 0; i < 20; i++) {
        idx[i] = new_idx[i];
    }
    free(new_idx);

    printf("after encoding: \n");
    for (i = 0; i < 20; i++) {
        printf("orig size is %d, new size is %d \n", arr[i].size, arr[i].new_size);
    }

    printf("\n");

    if (delta_decode(arr, 20, idx[0], &b[0]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    printf("\n");
    if (idx[0] != 0) {
        free(b[0]); // free space
    }

    //call delta_decode()
    if (delta_decode(arr, 20, idx[1], &b[1]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    if (idx[1] != 0) {
        free(b[1]); // free space
    }

    if (delta_decode(arr, 20, idx[2], &b[2]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    if (idx[2] != 0) {
        free(b[2]); // free space
    }
    
    //call delta_decode()
    if (delta_decode(arr, 20, idx[3], &b[3]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    if (idx[3] != 0) {
        free(b[3]); // free space
    }

    //call delta_decode()
    if (delta_decode(arr, 20, idx[4], &b[4]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    if (idx[4] != 0) {
        free(b[4]); // free space
    }

    //call delta_decode()
    if (delta_decode(arr, 20, idx[5], &b[5]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    if (idx[5] != 0) {
        free(b[5]); // free space 
    }

    printf("another around \n");
    printf("... \n");

    //call delta_decode()
    if (delta_decode(arr, 20, idx[0], &b[0]) == DELTA_DECODE_SUCCESS) {
        printf("delta decode succeeds.\n");
    }
    printf("\n");
    if (idx[0] != 0) {
        free(b[0]); // free space
    }

out:
    for (i = 0; i < 20; ++i) {
        if (arr[i].buf != NULL) {
            free(arr[i].buf); 
        }
    }
    return(0);
}


