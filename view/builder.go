package view

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
)

type designdoc struct {
	mapfn    string
	reducefn string
}

type viewindex struct {
	defn *ast.CreateIndexStatement
	ddoc designdoc
}

func NewViewIndex(stmt *ast.CreateIndexStatement) api.Accesser {
	inst := viewindex{
		defn: stmt,
		ddoc: *newDesignDoc(stmt),
	}
	return &inst
}

func newDesignDoc(stmt *ast.CreateIndexStatement) *designdoc {
	var doc designdoc
	return &doc
}

func verifyDesignDoc(doc *designdoc) bool {
	// TODO
	return true
}

func (this *viewindex) Name() string {
	return this.defn.Name
}

func (this *viewindex) Defn() *ast.CreateIndexStatement {
	return this.defn
}

func (this *viewindex) Type() api.IndexType {
	return api.View
}
