package main

import (
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/engine"
	"github.com/couchbaselabs/tuqtng/ast"
	"github.com/couchbaselabs/tuqtng/parser/goyacc"
)

func main() {
	var eng api.Indexer = engine.GetEngine("http://localhost:8091/")

	unql := "CREATE VIEW INDEX test ON beer-sample(name)"
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
	fmt.Println(paths)
}
