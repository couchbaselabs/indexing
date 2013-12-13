package btree

import "testing"
import "os"
import "strings"
import "math/rand"
import "time"
import "bufio"
import "fmt"

var KEYFILE = "./data/btree.key"
var WORDFILE = "./data/words"

var _ = fmt.Sprintf("keep 'fmt' import during debugging")

func TestBasic(t *testing.T) {
	var fpos [5]int64

	os.Create(KEYFILE)
	ks, _ := NewVStore(KEYFILE)
	defer func() {
		ks.Close()
		os.Remove(KEYFILE)
	}()

	fpos[0] = ks.Append("hello")
	fpos[1] = ks.Append("world")
	fpos[2] = ks.Append("how")
	fpos[3] = ks.Append([]byte{0, 1, 2})
	fpos[4] = ks.Append("end")

	if val, _ := ks.ReadStr(fpos[0]); val != "hello" {
		t.Fail()
	}
	if val, _ := ks.ReadStr(fpos[1]); val != "world" {
		t.Fail()
	}
	if val, _ := ks.ReadStr(fpos[2]); val != "how" {
		t.Fail()
	}
	if valb, _ := ks.Read(fpos[3]); fmt.Sprintf("%v", valb) != "[0 1 2]" {
		t.Fail()
	}
	if val, _ := ks.ReadStr(fpos[4]); val != "end" {
		t.Fail()
	}
}

func TestConcur(t *testing.T) {
	refwords := readlines(WORDFILE)
	posch := make(chan int64, 100)
	rnd := rand.New(rand.NewSource(int64(time.Now().Second())))
	os.Create(KEYFILE)
	ks, _ := NewVStore(KEYFILE)
	defer func() {
		ks.Close()
		os.Remove(KEYFILE)
	}()

	go doWrite(ks, refwords, posch)

	time.Sleep(100 * time.Millisecond)
	words := make([]string, len(refwords))
	fposs := make([]int64, len(refwords))
	for i := 0; true; i++ {
		fpos := <-posch
		if fpos == -1 {
			break
		}
		fposs[i] = fpos
		words[i], _ = ks.ReadStr(fpos)
		if i == 0 {
			continue
		}
		j := rnd.Intn(i)
		word, _ := ks.ReadStr(fposs[j])
		if word != words[j] {
			t.Fail()
		}
	}
	if strings.Join(words, " ") != strings.Join(refwords, " ") {
		t.Fail()
	}
}

func doWrite(ks *VStore, words []string, posch chan int64) {
	for _, word := range words {
		fpos := ks.Append(word)
		posch <- fpos
	}
	posch <- -1
}

func readlines(filename string) []string {
	var lines = make([]string, 0)
	fd, _ := os.Open(filename)
	defer func() { fd.Close() }()
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		lines = append(lines, string(scanner.Bytes()))
	}
	return lines
}
