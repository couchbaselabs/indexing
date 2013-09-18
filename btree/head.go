// Manages head sector of btree index-file. Head sector contains the following
// items,
//      rootFileposition int64
//      sectorsize int64
//      flistsize int64
//      blocksize int64
//      pick int64
//      crc uint32

package btree

import (
    "encoding/binary"
    "os"
    "bytes"
)

// Structure to manage the head sector
type Head struct {
    wstore *WStore
    dirty bool       // tells whether `root` has side-effects
    pick int64       // either 0 or 1, which freelist to pick. NOT USED !!
    sectorsize int64 // head sector-size in bytes.
    flistsize int64  // free-list size in bytes.
    blocksize int64  // btree block size in bytes.
    root int64       // file-offset into index file that has root block
    rootN Node       // root Node.
    fpos_head1 int64 // file-offset into index file where 1st-head is
    fpos_head2 int64 // file-offset into index file where 2nd-head is
    crc uint32       // CRC value for head sector + freelist block
}

// Create a new Head sector structure.
func newHead(wstore *WStore) *Head {
    hd := Head {
        wstore: wstore,
        pick: 0,
        sectorsize: wstore.Sectorsize,
        flistsize: wstore.Flistsize,
        blocksize: wstore.Blocksize,
        dirty: false,
        root: 0,
        fpos_head1: 0,
        fpos_head2: wstore.Sectorsize,
    }
    return &hd
}

// Fetch head sector from index file, read root block's file position and
// check whether head1 and head2 copies are consistent.
func (hd *Head) fetch() bool {
    LittleEndian := binary.LittleEndian
    if hd.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }
    rfd, _ := os.Open(hd.wstore.Idxfile)

    rfd.Seek(hd.fpos_head1, os.SEEK_SET) // Read from first sector
    if err := binary.Read(rfd, LittleEndian, &hd.root); err != nil {
        panic("Unable to read root from first head sector")
    }
    if err := binary.Read(rfd, LittleEndian, &hd.sectorsize); err != nil {
        panic("Unable to read sectorsize from first head sector")
    }
    if err:= binary.Read(rfd, LittleEndian, &hd.flistsize); err != nil {
        panic("Unable to read flistsize from first head sector")
    }
    if err:= binary.Read(rfd, LittleEndian, &hd.blocksize); err != nil {
        panic("Unable to read blocksize from first head sector")
    }
    if err:= binary.Read(rfd, LittleEndian, &hd.pick); err != nil {
        panic("Unable to read pick from first head sector")
    }
    if err:= binary.Read(rfd, LittleEndian, &hd.crc); err != nil {
        panic("Unable to read crc from first head sector")
    }

    data1 := make([]byte, hd.sectorsize)
    data2 := make([]byte, hd.sectorsize)

    if _, err := rfd.ReadAt(data1, hd.fpos_head1); err != nil {
        panic(err.Error())
    }
    if _, err := rfd.ReadAt(data2, hd.fpos_head2); err != nil {
        panic(err.Error())
    }
    if bytes.Equal(data1, data2) {
        return false
    }
    return true
}

// Refer to new root block. When ever an entry / block is updated the entire
// chain has to be re-added.
func (hd *Head) setRoot(fpos int64) *Head {
    hd.root = fpos
    hd.dirty = true
    return hd
}

// flush head-structure to index-file. Updates CRC for freelist.
func (hd *Head) flush(crc uint32) *Head {
    wfd := hd.wstore.idxWfd
    LittleEndian := binary.LittleEndian

    hd.crc = crc

    buf := bytes.NewBuffer([]byte{})
    binary.Write(buf, LittleEndian, &hd.root);
    binary.Write(buf, LittleEndian, &hd.sectorsize);
    binary.Write(buf, LittleEndian, &hd.flistsize);
    binary.Write(buf, LittleEndian, &hd.blocksize);
    binary.Write(buf, LittleEndian, &hd.pick);
    binary.Write(buf, LittleEndian, &hd.crc);

    valb := buf.Bytes()
    wfd.WriteAt(valb, hd.fpos_head2) // Write into head sector2
    wfd.WriteAt(valb, hd.fpos_head1) // Write into head sector1

    hd.dirty = false
    hd.wstore.flushHeads += 1
    return hd
}
