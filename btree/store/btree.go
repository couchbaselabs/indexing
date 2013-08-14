package store
import "os"
import "indexing/btree"

type BTreeStore struct {
    filename string
    wfd *os.File
    rfd *os.File
}

func NewBTreeStore() *BtreeStore {
}

func (bt *BTreeStore) GetBtreeRoot() {
}

func (bt *BTreeStore) PutBtreeRoot() {
}
