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

	go pumpMutationSet(1, 5000000)
	go pumpMutationSet(5000001, 10000000)
	go pumpMutationSet(10000001, 15000000)
	go pumpMutationSet(15000001, 20000000)
	go pumpMutationSet(20000001, 25000000)
	go pumpMutationSet(25000001, 30000000)
	go pumpMutationSet(30000001, 35000000)
	go pumpMutationSet(35000001, 40000000)
	go pumpMutationSet(40000001, 45000000)
	go pumpMutationSet(45000001, 50000000)
	go pumpMutationSet(50000001, 55000000)
	go pumpMutationSet(55000001, 60000000)
	go pumpMutationSet(60000001, 65000000)
	go pumpMutationSet(65000001, 70000000)
	go pumpMutationSet(70000001, 75000000)
	go pumpMutationSet(75000001, 80000000)
	go pumpMutationSet(80000001, 85000000)
	go pumpMutationSet(85000001, 90000000)
	go pumpMutationSet(90000001, 95000000)
	pumpMutationSet(95000001, 100000000)
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
