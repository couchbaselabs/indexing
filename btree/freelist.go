// Manages list of free blocks in btree index-file.
package btree

import (
    "encoding/binary"
    "fmt"
    "bytes"
    "hash/crc32"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Structure to manage the free list
type FreeList struct {
    wstore *WStore
    dirty bool        // Tells whether `freelist` contain side-effects
    fpos_block1 int64 // file-offset into index file where 1st-list is
    fpos_block2 int64 // file-offset into index file where 2nd-list is
    offsets []int64   // array(slice) of free blocks
}

var crctab = crc32.MakeTable(crc32.IEEE)

// Create a new FreeList structure
func newFreeList(wstore *WStore) *FreeList {
    fl := FreeList {
        wstore: wstore,
        dirty: false,
        fpos_block1: wstore.Sectorsize*2,
        fpos_block2: wstore.Sectorsize*2 + wstore.Flistsize,
        offsets: make([]int64, 1, wstore.maxFreeBlocks()),  // lastblock is zero
    }
    return &fl
}

// Fetch list of free blocks from index file. 
func (fl *FreeList) fetch(crc uint32) bool {
    var fpos int64
    if fl.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }

    // Open the index file in read mode.
    wstore := fl.wstore
    rfd, _ := os.Open(wstore.Idxfile)

    // Read the first block
    bytebuf := make([]byte, wstore.Flistsize)
    if _, err := rfd.ReadAt(bytebuf, fl.fpos_block1); err != nil {
        panic(err.Error())
    }
    // Load the offsets
    fl.offsets = fl.offsets[:0]
    buf := bytes.NewBuffer(bytebuf)
    for i := 0; i < wstore.maxFreeBlocks(); i++ {
        binary.Read(buf, binary.LittleEndian, &fpos)
        fl.offsets = append(fl.offsets, int64(fpos)) // include zero-terminator
        if fpos == 0 {
            break
        }
    }

    // verify the crc.
    crc1 := crc32.Checksum(bytebuf, crctab)
    if crc != crc1 {
        return false
    }

    // Verify with the second block
    bytebuf_ := make([]byte, wstore.Flistsize)
    if _, err := rfd.ReadAt(bytebuf_, fl.fpos_block2); err != nil {
        panic(err.Error())
    }
    return bytes.Equal(bytebuf, bytebuf_)
}

// Add a list of offsets to free blocks. By adding `offsets` into the
// freelist, length of freelist must not exceed `maxFreeBlocks()+1`.
func (fl *FreeList) add(offsets []int64) *FreeList {
    ln := len(fl.offsets)
    if (ln + len(offsets)) <= fl.wstore.maxFreeBlocks() {
        fl.offsets = append(fl.offsets[:ln-1], offsets...)
        fl.offsets = append(fl.offsets, 0) // Zero terminator
        fl.dirty = true
    } else {
        panic("Cannot add more than maxFreeBlocks()")
    }
    return fl
}

// Get a freeblock
func (fl *FreeList) pop() int64 {
    if fl.offsets[0] == 0 {
        panic("Freelist is not expected to go empty")
    }
    fpos := fl.offsets[0]
    fl.offsets = fl.offsets[1:]
    fl.wstore.popCounts += 1 // stats
    return fpos
}

func (fl *FreeList) flush() uint32 {
    buf := bytes.NewBuffer([]byte{})
    // Zero fill offsets
    count := fl.wstore.maxFreeBlocks() - len(fl.offsets)
    offsets := append(fl.offsets, make([]int64, count)...)
    // Dump offsets
    for _, fpos := range offsets {
        binary.Write(buf, binary.LittleEndian, &fpos)
    }
    bytebuf := buf.Bytes()
    // Write into the second copy
    wfd := fl.wstore.idxWfd
    wfd.WriteAt(bytebuf, fl.fpos_block2) // Write the second copy
    wfd.WriteAt(bytebuf, fl.fpos_block1) // Write the first copy

    fl.wstore.flushFreelists += 1
    fl.dirty = false

    crc := crc32.Checksum(bytebuf, crctab)
    return crc
}

func (fl *FreeList) isCritical() bool {
    return len(fl.offsets) < (2*fl.wstore.Maxlevel)
}

func (fl *FreeList) limit() int {
    limit := int(float32(fl.wstore.maxFreeBlocks()) * fl.wstore.AppendRatio)
    if limit < 100 {
        limit = fl.wstore.maxFreeBlocks()
    }
    return limit
}
