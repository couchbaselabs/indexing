package btree

import (
    "bytes"
    "os"
    "fmt"
    "math/rand"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");
var datafile = "./data/appendkv_datafile.dat"

func Test_KV(t *testing.T) {
    os.Create(datafile)
    wfd, _ := os.OpenFile(datafile, os.O_APPEND | os.O_WRONLY, 0660)
    rfd, _ := os.Open(datafile)
    defer func() {
        wfd.Close()
        rfd.Close()
        os.Remove(datafile)
    }()
    fpos := appendKV( wfd, []byte("Hello world") )
    if bytes.Equal(readKV(rfd, fpos), []byte("Hello world")) == false {
        t.Fail()
    }
}

func Benchmark_appendKV(b *testing.B) {
    data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
    data += data
    data += data
    os.Create(datafile)
    wfd, _ := os.OpenFile(datafile, os.O_APPEND | os.O_WRONLY, 0660)
    defer func() {
        wfd.Close()
        os.Remove(datafile)
    }()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        appendKV(wfd, []byte(data))
    }
}

var fposs = make([]int64, 0)
var maxEntries = 100000000
func Benchmark_fetchSetup(b *testing.B) {
    data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
    data += data
    data += data
    os.Create(datafile)
    wfd, _ := os.OpenFile(datafile, os.O_APPEND | os.O_WRONLY, 0660)
    defer func() {
        wfd.Close()
    }()
    for i := 0; i < maxEntries; i++ {
        fposs = append(fposs, appendKV(wfd, []byte(data)))
    }
    b.SkipNow()
}

func Benchmark_fetchKV(b *testing.B) {
    rfd, _ := os.Open(datafile)
    defer func() {
        rfd.Close()
    }()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        readKV(rfd, fposs[rand.Intn(maxEntries)])
    }
}

func Benchmark_fetchFinish(b *testing.B) {
    os.Remove(datafile)
}
