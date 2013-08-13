package store
import "os"
import "fmt"
import "encoding/binary"

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

type VStore struct {
    wfd *os.File
    rfd *os.File
    writech chan interface{}
    // Add `filename` field.
}

func NewVStore(filename string) (*VStore, error) {
    rfd, err := os.Open(filename)
    wfd, err := os.OpenFile(filename, os.O_WRONLY, os.ModeAppend)
    wfd.Seek(0, os.SEEK_END)
    writech := make(chan interface{})
    go appendOnly(wfd, writech)
    return &VStore{wfd:wfd, rfd:rfd, writech: writech}, err
}

func (f *VStore) Append(val interface{}) int64 {
    f.writech <- val
    rc := <-f.writech
    err, ok := rc.(error)
    if ok {
        panic(err.Error())
    }
    fpos, ok := rc.(int64)
    if ok == false {
        panic("Expecting file-position !!")
    }
    return fpos
}

func (f *VStore) Read(fpos int64) ([]byte, error) {
    return readBytes(f, fpos)
}

func (f *VStore) ReadStr(fpos int64) (string, error) {
    bin, err := readBytes(f, fpos)
    return string(bin), err
}

func (f *VStore) Close() {
    f.wfd.Close()
    f.rfd.Close()
    close( f.writech )
}

//---- Local functions

func readBytes(f *VStore, fpos int64) ([]byte, error) {
    var size int32
    newfpos, err := f.rfd.Seek(fpos, os.SEEK_SET)
    if err != nil {
        return nil, err
    } else if newfpos != fpos {
        panic("New offset does not match requested offset")
    }
    binary.Read(f.rfd, binary.LittleEndian, &size)
    b := make([]byte, size)
    _, err = f.rfd.Read( b )
    if err != nil {
        return nil, err
    }
    return b, nil
}

