package main

import (
	ast "github.com/couchbaselabs/tuqtng/ast"
	"testing"
)

var doc = []byte(`{"name":"Fireman's Pail Ale","abv":0.0,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"pennichuck_brewi ng_company","updated":"2010-07-22 20:00:20","description":"","style":"American-Style Pale Ale","category":"North American Ale"}`)

func BenchmarkEvaluate(b *testing.B) {
	expr := `{"type":"property","path":"name"}`
	ex, err := ast.UnmarshalExpression([]byte(expr))
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		evaluate(doc, []ast.Expression{ex})
	}
}
