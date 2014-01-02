package main

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/prataprc/golib"
	"github.com/prataprc/monster"
	"math/rand"
)

func populateUsers(bucket *couchbase.Bucket) {
	// Map of interfaces can receive any value types
	value := map[string]interface{}{}

	if options.userProd == "" {
		return
	}

	conf := make(golib.Config)
	start := monster.Parse(options.userProd, conf)

	c := make(monster.Context)
	nonterminals, root := monster.Build(start)
	c["_random"] = rand.New(rand.NewSource(int64(options.seed)))
	c["_nonterminals"] = nonterminals
	for i := 0; i < options.count; i++ {
		jsbytes := []byte(root.Generate(c))
		if err := json.Unmarshal(jsbytes, &value); err != nil {
			panic(err)
		}
		docid := fmt.Sprintf("user%v", i)
		bucket.Set(docid, 0, value)
	}
}

func populateProjects(bucket *couchbase.Bucket) {
	// Map of interfaces can receive any value types
	value := map[string]interface{}{}

	if options.projProd == "" {
		return
	}

	conf := make(golib.Config)
	start := monster.Parse(options.projProd, conf)

	c := make(monster.Context)
	nonterminals, root := monster.Build(start)
	c["_random"] = rand.New(rand.NewSource(int64(options.seed)))
	c["_nonterminals"] = nonterminals
	for i := 0; i < options.count; i++ {
		jsbytes := []byte(root.Generate(c))
		if err := json.Unmarshal(jsbytes, &value); err != nil {
			panic(err)
		}
		docid := fmt.Sprintf("project%v", i)
		bucket.Set(docid, 0, value)
	}
}
