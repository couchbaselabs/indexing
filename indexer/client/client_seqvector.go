package main

import (
	"github.com/couchbaselabs/indexing/api"
	"log"
	"net"
	"net/rpc/jsonrpc"
)

func main() {

	conn, err := net.Dial("tcp", "localhost:8096")

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := jsonrpc.NewClient(conn)

	var indexList api.IndexList
	var returnMap api.IndexSequenceMap

	err = c.Call("MutationManager.GetSequenceVector", &indexList, &returnMap)
	if err != nil {
		log.Fatal("Mutation error:", err)
	}
	log.Printf("Mutate Response: %v", returnMap)
}
