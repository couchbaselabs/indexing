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
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"log"
	"net"
	"net/rpc/jsonrpc"
)

func main() {

	doc := `{
             "type":"order",
             "id":1300,
             "bool":true,
             "boolstr":"true",
             "shipped-on":null,
             "orderlines":[
                      {
                       "qty": 1,
                       "productId": "coffee01"
                      },
                      {
                       "qty": 1,
                       "productId": "sugar22"
                      }
                     ],
             "map" :{ "k1" : "v1", "k2" : true }
          }`

	input := []string{
		//        `{"type":"property","path":"orderlines"}`,
		`{"type":"property","path":"id"}`,
		//      `{"type":"property","path":"bool"}`,
		//      `{"type":"property","path":"boolstr"}`,
		//      `{"type":"property","path":"map"}`,
		//      `{"type":"property","path":"shipped-on"}`,
	}

	insert := &api.Mutation{
		Type:         "INSERT",
		Indexid:      "5c07456c-3256-4099-78c0-3aebfc4bdef6",
		SecondaryKey: make([][]byte, 0),
		Docid:        "doc1",
		Vbucket:      1,
		Seqno:        1,
	}
	/*
	   delete := &api.Mutation{
	       Type:   "DELETE",
	       Indexid: "2049bff0-2638-403c-6c6a-853cf792f5ee",
	       SecondaryKey: make([][]byte, 0),
	       Docid:        "doc1",
	       Vbucket:      1,
	       Seqno:        1,
	   }
	*/
	for _, v := range input {
		expr, err := ast.UnmarshalExpression([]byte(v))
		log.Printf("%v %v", expr, err)
		val, err := expr.Evaluate(dparval.NewValueFromBytes([]byte(doc)))
		log.Printf("%v %v %v", val.Value(), val.Bytes(), err)
		insert.SecondaryKey = append(insert.SecondaryKey, val.Bytes())
	}

	conn, err := net.Dial("tcp", "localhost:8096")

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var reply bool

	c := jsonrpc.NewClient(conn)

	err = c.Call("MutationManager.ProcessSingleMutation", insert, &reply)
	//  err = c.Call("MutationManager.ProcessSingleMutation", delete, &reply)
	if err != nil {
		log.Fatal("Mutation error:", err)
	}
	log.Printf("Mutate Response: %v", reply)
}
