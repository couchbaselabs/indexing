package main

import (
	"github.com/couchbaselabs/tuqtng/ast"
	"github.com/couchbaselabs/tuqtng/parser/goyacc"	
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/engine"
	"fmt"
)

func main() {
	var eng api.Indexer = engine.GetEngine()
	
	unql := "CREATE VIEW INDEX test ON contacts(name)"
	parser := goyacc.NewUnqlParser()
	stmt, err := parser.Parse(unql)
	if err != nil {
		panic(err)
	}
	
	eng.Create(stmt.(*ast.CreateIndexStatement))
	paths := eng.Indexes()
	fmt.Println(paths)
}
