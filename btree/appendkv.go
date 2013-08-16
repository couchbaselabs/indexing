package btree
import "os"
import "fmt"
import "encoding/binary"

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func appendOnly( wfd *os.File, writech chan interface{} ) {
    var err error
    var fpos int64
    for {
        val := <-writech
        if val == nil {
            break
        }
        switch val.(type) {
        case []byte :
            fpos, err = writeBytes(wfd, val.([]byte))
        case string :
            fpos, err = writeStr(wfd, val.(string))
        }
        if err == nil {
            writech <- fpos
        } else {
            writech <- err
        }
    }
}

func writeBytes(wfd *os.File, val []byte) (int64, error) {
    fpos, err := wfd.Seek(0, os.SEEK_CUR)
    if err != nil {
        return -1, err
    }

    size := int32( len(val) )
    binary.Write(wfd, binary.LittleEndian, &size)
    if _, err := wfd.Write( val ); err != nil {
        return -1, err
    }
    return fpos, nil
}

func writeStr(wfd *os.File, val string) (int64, error) {
    return writeBytes( wfd, []byte( val ))
}
