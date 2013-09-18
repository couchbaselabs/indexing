// Common functions used across test cases.
package btree

import (
    "os"
    "fmt"
    "bytes"
    "math/rand"
    "time"
    "bufio"
)

var _ = fmt.Sprintln(time.Now())

var testconf1 = Config{
    Idxfile: "./data/index_datafile.dat",
    Kvfile: "./data/appendkv_datafile.dat",
    IndexConfig: IndexConfig{
        Sectorsize: 512,
        Flistsize: 1000 * OFFSET_SIZE,
        Blocksize: 64*1024,
    },
    Maxlevel: 6,
    RebalanceThrs: 6,
    AppendRatio: 0.7,
    Sync: false,
    Nocache: false,
}

type TestKey struct {
    K string
    Id int
}
type TestValue struct {
    V string
}

func (tk *TestKey) Bytes() []byte {
    return []byte(tk.K)
}

func (tk *TestKey) Docid() []byte {
    return []byte(fmt.Sprintf("%020v", tk.Id))
}

func (tk *TestKey) Control() uint32 {
    return 0
}

func (tk *TestKey) Less(otherk []byte, otherd []byte) bool {
    kcomp := bytes.Compare(tk.Bytes(), otherk)
    if kcomp < 0 {
        return true
    } else if kcomp == 0 && bytes.Compare(tk.Docid(), otherd) < 0 {
        return true
    }
    return false
}

func (tk *TestKey) LessEq(otherk []byte, otherd []byte) bool {
    kcomp := bytes.Compare(tk.Bytes(), otherk)
    if kcomp < 0 {
        return true
    } else if kcomp == 0 && bytes.Compare(tk.Docid(), otherd) < 1 {
        return true
    }
    return false
}

func (tk *TestKey) Equal(otherk []byte, otherd []byte) (bool, bool) {
    var keyeq, doceq bool
    if otherk == nil {
        keyeq = false
    } else {
        keyeq = bytes.Equal(tk.Bytes(), otherk)
    }
    if otherd == nil {
        doceq = false
    } else {
        doceq = bytes.Equal(tk.Docid(), otherd)
    }
    return keyeq, doceq
}

func (tv *TestValue) Bytes() []byte {
    return []byte(tv.V)
}

func TestData(count int, seed int64) ([]*TestKey, []*TestValue) {
    if seed < 0 {
        seed = int64(time.Now().Nanosecond())
    }
    rnd := rand.New(rand.NewSource(seed))

    keys := make([]*TestKey, 0, count)
    values := make([]*TestValue, 0, count)
    for i := 0; i < count; i++ {
        keys = append(keys, &TestKey{RandomKey(rnd), i})
        values = append(values, &TestValue{RandomValue(rnd)+"Value"})
    }
    return keys, values
}

func testStore(remove bool) *Store {
    if remove {
        os.Remove("./data/index_datafile.dat")
        os.Remove("./data/appendkv_datafile.dat")
    }
    return NewStore(testconf1)
}

var keys = make([]string, 0)
func RandomKey(rnd *rand.Rand) string {
    if len(keys) == 0 {
        fd, _ := os.Open("./data/words")
        scanner := bufio.NewScanner(fd)
        for scanner.Scan() {
            keys = append(keys, scanner.Text())
        }
    }
    return keys[rnd.Intn(len(keys))]
}

func RandomValue(rnd *rand.Rand) string {
    return RandomKey(rnd)
}
