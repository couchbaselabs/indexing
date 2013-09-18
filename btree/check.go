package btree

import (
    "os"
)

func (store *Store) check() bool {
    wstore := store.wstore
    freelist := wstore.freelist
    rfd, _ := os.Open(wstore.Idxfile)

    // Check whether configuration settings match.
    fi, _ := rfd.Stat()
    blksize := fi.Size() - (wstore.Sectorsize*2) - (wstore.Flistsize*2)
    if ((wstore.Sectorsize*2) + (wstore.Flistsize*2)) != wstore.fpos_firstblock{
        return false
    }
    if blksize % int64(wstore.Blocksize) != 0 {
        return false
    }

    // Check freelist with btree.
    root, _, _ := store.Root(false)
    offs := root.listOffsets()
    qsortOffsets(offs)
    fulloffs := seq(wstore.fpos_firstblock, fi.Size(), int64(wstore.Blocksize))
    offsets := make([]int64, 0, len(fulloffs))
    i := 0
    for _, x := range offs {
        for i < len(fulloffs) {
            if fulloffs[i] < x {
                offsets = append(offsets, fulloffs[i])
            } else if fulloffs[i] > x {
                break
            }
            i++
        }
    }
    for ; i < len(fulloffs); i++ {
        offsets = append(offsets, fulloffs[i])
    }
    offsets = append(offsets, 0)
    if len(offsets) != len(freelist.offsets) {
        return false
    }

    count := 0
    for _, offset := range offsets {
        for _, floffset := range freelist.offsets {
            if offset == floffset {
                count += 1
                break
            }
        }
    }
    if count != len(offsets) {
        return false
    }
    return true
}

// Inplace quicksort
func qsortOffsets(arr []int64) {
    if len(arr) <= 1 {
        return
    }
    iPivot := qsort_partition(arr)
    qsortOffsets(arr[:iPivot])
    qsortOffsets(arr[iPivot-1:])
}

func qsort_partition(arr []int64) int {
    swap := func(arr []int64, i1, i2 int) {
        arr[i1], arr[i2] = arr[i2], arr[i1]
    }

    idx, lastidx := 0, len(arr)-1
    pivot := arr[lastidx] // rightmost element
    for i := 1; i < len(arr); i++ {
        if arr[i] < pivot {
            swap(arr, i, idx)
            idx++
        }
    }
    swap(arr, lastidx, idx)
    return idx
}

func seq(start, end, step int64) []int64 {
    out := make([]int64, 0, (end-start)/step)
    for i := start; i < end; i += step {
        out = append(out, i)
    }
    return out
}

