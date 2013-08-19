package main

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/engine"
	"fmt"
)

func main() {
	var eng api.Indexer = engine.GetEngine()
	eng.Create(nil)
	eng.Create(nil)
	eng.Create(nil)
	
	paths := eng.Instances()
	fmt.Println(len(paths))
}
