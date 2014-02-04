//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
