package view

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"regexp"
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
	err := generateJS(stmt, &doc)
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

func generateJS(stmt *ast.CreateIndexStatement, doc *designdoc) error {
	// TODO
	doc.mapfn = `function (doc, meta) {emit(meta.id, null);}`
	doc.reducefn = ``
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
	fmt.Println("tl", tl, "tr", tr)
	return tr == tl
}

var buckets map[string]*couchbase.Bucket = make(map[string]*couchbase.Bucket)

func getBucketForIndex(idx *viewindex) (*couchbase.Bucket, error) {

	if cached := buckets[idx.url]; cached != nil {
		return cached, nil
	}

	cb, err := couchbase.Connect(idx.url)
	if err != nil {
		return nil, err
	}

	pool, err := cb.GetPool("default")
	if err != nil {
		return nil, err
	}

	bucket, err := pool.GetBucket(idx.defn.Bucket)
	if err != nil {
		return nil, err
	}

	buckets[idx.url] = bucket
	return bucket, nil
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
