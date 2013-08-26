package main

import (
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/engine"
	"github.com/couchbaselabs/tuqtng/ast"
	"github.com/couchbaselabs/tuqtng/parser/goyacc"
	"time"
)

func main() {
	var eng api.Indexer = engine.GetEngine("http://localhost:8091/")

	unql := "CREATE INDEX tester ON beer-sample(name, abv) USING view"
	parser := goyacc.NewUnqlParser()
	stmt, err := parser.Parse(unql)
	if err != nil {
		panic(err)
	}

	err = eng.Create(stmt.(*ast.CreateIndexStatement))
	if err != nil {
		panic(err)
	}

	paths := eng.Indexes()
	for _, name := range paths {
		path := eng.Index(name)
		fmt.Println(path.Name())
	}

	time.Sleep(3 * time.Second)

	unql = "DROP INDEX beer-sample.tester"
	stmt, err = parser.Parse(unql)
	if err != nil {
		panic(err)
	}

	err = eng.Drop(stmt.(*ast.DropIndexStatement))
	if err != nil {
		panic(err)
	}
}
