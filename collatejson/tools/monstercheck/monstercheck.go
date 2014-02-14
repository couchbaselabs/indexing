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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/indexing/collatejson"
	tuqcollate "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/monster"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
)

var options struct {
	prodfile string
	count    int
	seed     int
	nfkd     bool
	utf8     bool
}

type codeList struct {
	kind  string
	jsons []string
}

var codec *collatejson.Codec

func argParse() {
	flag.StringVar(&options.prodfile, "p", "json.prod",
		"production file to use")
	flag.IntVar(&options.count, "c", 100,
		"Number samples")
	flag.IntVar(&options.seed, "seed", 0,
		"Random seed")
	flag.BoolVar(&options.nfkd, "nfkd", false,
		"use decomposed canonical normalization for unicode collation")
	flag.BoolVar(&options.utf8, "utf8", false,
		"use plain string for unicode collation")
	flag.Parse()
}

func main() {
	argParse()
	if options.seed == 0 {
		options.seed = int(time.Now().UnixNano())
	}

	codec = collatejson.NewCodec()
	if options.nfkd {
		codec.SortbyNFKD(true)
	}
	if options.utf8 {
		codec.SortbyUTF8(true)
	}

	fmt.Printf("Generating %v json documents ...\n", options.count)
	jsons := generateJsons(options.prodfile, options.seed, options.count)
	checkCodec(jsons)
	fmt.Println("Done")
}

func checkCodec(jsons []string) {
	var one, two interface{}

	fmt.Println("Checking Encoding and Decoding ...")
	for _, j := range jsons {
		out := codec.Decode(codec.Encode([]byte(j)))
		json.Unmarshal([]byte(j), &one)
		json.Unmarshal(out, &two)
		if !reflect.DeepEqual(one, two) {
			panic("monster check fails, did you change the encoding format ?")
		}
	}
	fmt.Println()
}

func compareWithTuq(jsons []string, count int) {
	for i := int(0); i < count; i++ {
		fmt.Printf(".")
		tuqjsons := make([]string, len(jsons))
		copy(tuqjsons, jsons)
		binjsons := make([]string, len(jsons))
		copy(binjsons, jsons)

		tuqcodes := codeList{"tuq", tuqjsons}
		sort.Sort(tuqcodes)
		fd, _ := os.Create("a")
		fd.Write([]byte(strings.Join(tuqcodes.jsons, "\n")))
		fd.Close()

		bincodes := codeList{"binary", binjsons}
		sort.Sort(bincodes)
		fd, _ = os.Create("b")
		fd.Write([]byte(strings.Join(bincodes.jsons, "\n")))
		fd.Close()
	}
	fmt.Println()
}

func generateJsons(prodfile string, seed, count int) []string {
	jsons, err := monster.Generate(seed, count, "", prodfile)
	if err != nil {
		panic(err)
	}
	return jsons
}

func (codes codeList) Len() int {
	return len(codes.jsons)
}

func (codes codeList) Less(i, j int) bool {
	key1, key2 := codes.jsons[i], codes.jsons[j]
	if codes.kind == "tuq" {
		value1 := dparval.NewValueFromBytes([]byte(key1)).Value()
		value2 := dparval.NewValueFromBytes([]byte(key2)).Value()
		return tuqcollate.CollateJSON(value1, value2) < 0
	} else if codes.kind == "binary" {
		value1 := codec.Encode([]byte(key1))
		value2 := codec.Encode([]byte(key2))
		return bytes.Compare(value1, value2) < 0
	} else {
		panic(fmt.Errorf("unknown kind"))
	}
	return false
}

func (codes codeList) Swap(i, j int) {
	codes.jsons[i], codes.jsons[j] = codes.jsons[j], codes.jsons[i]
}
