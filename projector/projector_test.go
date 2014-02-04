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
