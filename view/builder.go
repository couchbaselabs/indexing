package view

import (
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"regexp"
	"fmt"
	"bytes"
)

type viewindex struct {
	defn *ast.CreateIndexStatement
	ddoc *designdoc
	url  string
}

type designdoc struct {
	mapfn    string
	reducefn string
}

func NewViewIndex(stmt *ast.CreateIndexStatement, url string) (*viewindex, error) {
	doc, err := newDesignDoc(stmt, url)
	if err != nil {
		return nil, err
	}
	inst := viewindex{
		defn: stmt,
		ddoc: doc,
		url:  url,
	}

	return &inst, nil
}

func newDesignDoc(stmt *ast.CreateIndexStatement, url string) (*designdoc, error) {
	var doc designdoc
	err := generateMap(stmt, &doc)
	if err != nil {
		return nil, err
	}
	fmt.Println(doc.mapfn)
	err = generateReduce(stmt, &doc)
	if err != nil {
		return nil, err
	}
	fmt.Println(doc.reducefn)
	return &doc, nil
}

func generateMap(stmt *ast.CreateIndexStatement, doc *designdoc) error {
	buf := new(bytes.Buffer)
	leader := ""
	fmt.Fprintln(buf, leader, "function (doc, meta) {")
	leader = "  "
	
	vals := new(bytes.Buffer)
	for idx, expr := range stmt.On {
		walker := NewWalker()
		_, err := walker.Visit(expr)
		if err != nil {
			panic(err)
		}

		jvar := fmt.Sprintf("val%v", idx + 1)
		if (vals.Len() > 0) {		
			fmt.Fprintf(vals, "%s", ", ")
		}

		fmt.Fprintf(vals, "%s", jvar)		
		fmt.Fprintln(buf, leader, "var", jvar, "=", walker.JS() + ";")  
	}
	
	leader = ""
	fmt.Fprintln(buf, leader, "}")
	doc.mapfn = buf.String()
	return nil
}

func generateReduce(stmt *ast.CreateIndexStatement, doc *designdoc) error {
	// TODO
	doc.reducefn = ""
	return nil
}

func (idx *viewindex) verifyDesignDoc() error {
	bucket, err := getBucketForIndex(idx)
	if err != nil {
		return err
	}

	var ddoc couchbase.DDocJSON
	if err := bucket.GetDDoc(ddocName(idx), &ddoc); err != nil {
		return err
	}

	view, ok := ddoc.Views[viewName(idx)]
	if !ok {
		return api.DDocChanged
	}

	if !sameCode(view.Map, idx.ddoc.mapfn) {
		return api.DDocChanged
	}

	if !sameCode(view.Reduce, idx.ddoc.reducefn) {
		return api.DDocChanged
	}

	return nil
}

func ddocName(idx *viewindex) string {
	return "dev_" + idx.Name()
}

func viewName(idx *viewindex) string {
	return "autogen"
}

func sameCode(left, right string) bool {
	rx, _ := regexp.Compile(`\s+`)
	tl := rx.ReplaceAllLiteralString(left, "")
	tr := rx.ReplaceAllLiteralString(right, "")
	return tr == tl
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
