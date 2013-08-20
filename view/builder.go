package view

import (
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	//"fmt"
)

const (
	viewPrefix = "autogen_"
	ddocPrefix = "_design/dev_" + viewPrefix
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

func NewViewIndex(stmt *ast.CreateIndexStatement, url string) (api.Accesser, error) {
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
	return nil
}

func verifyDesignDoc(idx *viewindex) error {

	return nil
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
