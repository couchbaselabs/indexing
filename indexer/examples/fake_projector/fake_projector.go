package main

import (
	"github.com/couchbaselabs/indexing/api"
	"log"
	"math/rand"
	"net"
	"net/rpc/jsonrpc"
	"strconv"
	"time"
)

func main() {

	batchSize := 20000000
	numWorkers := 20

	for i := 0; i < numWorkers; i++ {
		if i < numWorkers-1 {
			go pumpMutationSet(i*batchSize+1, (i+1)*batchSize)
		} else {
			pumpMutationSet(i*batchSize+1, (i+1)*batchSize)
		}
	}

}

func pumpMutationSet(startNum int, endNum int) {

	var doneCount int

	insert := &api.Mutation{
		Type:         "UPR_MUTATION",
		Indexid:      "d6b4d5d3-d643-429e-4aa5-2a61e049774b",
		SecondaryKey: make([][]byte, 1),
		Docid:        "",
		Vbucket:      0,
		Seqno:        0,
	}

	conn, err := net.Dial("tcp", "10.144.14.198:8096")

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var reply bool
	c := jsonrpc.NewClient(conn)

	for x := startNum; x <= endNum; x++ {
		insert.Docid = "doc" + strconv.Itoa(x)
		insert.Seqno = uint64(x)
		insert.Vbucket = uint16(x % 1024)
		r := rand.Int31()
		insert.SecondaryKey[0] = []byte(strconv.Itoa(int(r)))
		c.Call("MutationManager.ProcessSingleMutation", insert, &reply)
		if doneCount%50000 == 0 || x == startNum {
			log.Printf("Sent Mutation %v at %v", insert.Docid, time.Now())
		}
		doneCount += 1
	}
}
