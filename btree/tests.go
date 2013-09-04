// Common functions used across test cases.
package btree

import (
    "os"
)

var testconf = Config{
    idxfile: "./data/index_datafile.dat",
    kvfile: "./data/appendkv_datafile.dat",
    sectorsize: SECTOR_SIZE,
    flistsize: FLIST_SIZE,
    blocksize: BLOCK_SIZE,
}

func testStore() *Store {
    os.Remove(testconf.idxfile)
    os.Remove(testconf.kvfile)
    return Create(testconf)
}
