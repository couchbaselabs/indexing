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

Here are some documentation materials/manuals for new compression modules (delta 
encoding, dictionary coding) that can be used for in-memory compression.

If you have any questions, feel free to CONTACT: fulu@couchbase.com

Purpose: The compression modules can be used to compress Doc metadata in memory, 
in particualr for DocID (16 bytes), CasID (8 bytes), Seqno
(8 bytes) using delta encoding/decoding, for item expiration time (4 bytes) and 
lock expiration time (4 bytes) using symbol table compression. For items that are
loaded in the same batch have the same expiration times. 

Here are the explanations/manuals and descriptions on the usage of the compression 
modules (delta encoding/decoding, symbol table compression) APIs:

(1)
    delta_encode_error_t delta_encode(sized_buf *arr,
                                      uint16_t len,
                                      uint16_t **new_idx)

Usage: arr is the pointer to the array of sized_buf elements, len is the number of 
       elements in the array, new_idx is the pointer to the array of the new index 
       values due to the sorting.

       If the return code is DELTA_ENCODE_SUCCESS, the caller need to free the new_idx
       after processing (setting the values to the original element structure,
       After setting new_idx for each metadata entry, free (new_idx);)

       For integration with ep engine, this arr should be declared in a commonly 
       accessible place (global) by all storedItem instances. The placeholder can 
       be two dimensional with the limit of each chunk is less than 64K items. 
       Following the way we are dealing with hashtable in ep engine
       We can keep this array the same way as the hash table in ep-engine as a global 
       variable

(2)
    delta_decode_error_t delta_decode(const sized_buf *arr,
                                      uint16_t len,
                                      uint16_t idx,
                                      char **buf)

Usage: arr is the pointer to the array of sized_buf elements, len is the number of elements
       in the array, idx is the value of the index in the array to be decompressed and the
       decompressed value will be in buf.

       The decompressed value is in the buf, if the return code is DELTA_DECODE_SUCCESS,
       the caller needs to free buf after usage if the idx is not zero (not the first entry)
       in the array (DO NOT free buf if the idx is ZERO, no compression is done for the 1st
       entry).
(3)
    sym_table_error_t st_init(sym_table_t **st)

Usage: to initialize a symbol table, the newly-created symbol table will be in st.


(4)
    sym_table_error_t st_insert(sym_table_t **st,
                                sized_buf *sym)

Usage: to insert a symbol of sym into the symbol table of st.


(5)
    sized_buf st_lookup_s2t(sym_table_t *st,
                            sized_buf *sym)

Usage: given a symbol of sym and the corresponding symbol table of st, return the token if there
       is a match, otherwise, returns size of 0 and buf of NULL for the sized_buf.

(6)
    sized_buf st_lookup_t2s(sym_table_t *st,
                            sized_buf *tok)

Usage: given a token of tok and the corresponding symbol table of st, return the symbol if there
       is a match, otherwise, returns size of 0 and buf of NULL for the sized_buf.


(7)
    void st_cleanup(sym_table_t **st)

Usage: to clean up those symbols without a token in symbol table of st
       when the ratio of num_of_total and num_of_tokens reaches a given
       threshold or the num_of_total exceeds a given threshold

(8)
    void st_free(sym_table_t *st)
Usage: to free allocated memory for the given symbol table of st.

    Note that both the symbols and the tokens have variable sizes, that is why we use sized_buf
    if the token size is 1, then replace the original symbol (seqno, item exp time, lock exp time)
    with an uint8_t, if the token size is 2, then replace the original symbol with an uint16_t.

    The way to use symbol table compression modules is to call st_init() to initialize a symbol
    table, call st_insert() to insert a symbol to the symbol table, then call st_lookup_s2t() to
    get the token for the given symbol, likewise call st_lookup_t2s() to get the symbol for the
    given token. st_cleanup() is triggered automatically inside st_insert().

    Finally, when all is done, call st_free() to free the allocated memory.

(9) Tests:
    (a) Compile delta.c and delta_test.c together to run the simple tests for delta 
        encoding/decoding modules: most of the logic paths are covered and some corner cases may 
        not be covered.
    (b) Compile dict.c and dict_test.c together to run the simple tests for dictionary
        coding compression modules: most of the logic paths are covered and some corner cases may
        not be covered.
 
