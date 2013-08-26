package view

import (
	"bytes"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"strings"
	"hash/crc32"
)

type DDocIndex struct {
	couchbase.DDocJSON
	DDLDefinition string `json:"ddlDefinition,omitempty"`
	DDLChecksum int      `json:"ddlChecksum,omitempty"`
}


type ViewIndex struct {
	defn *ast.CreateIndexStatement
	ddoc *designdoc
	url  string
}

type designdoc struct {
	mapfn    string
	reducefn string
}

func NewViewIndex(stmt *ast.CreateIndexStatement, url string) (*ViewIndex, error) {

	doc, err := newDesignDoc(stmt, url)
	if err != nil {
		return nil, err
	}

	inst := ViewIndex{
		defn: stmt,
		ddoc: doc,
		url:  url,
	}

	err = inst.putDesignDoc()
	if err != nil {
		return nil, err
	}

	return &inst, nil
}

func newDesignDoc(stmt *ast.CreateIndexStatement, url string) (*designdoc, error) {
	var doc designdoc

	err := generateMap(stmt, &doc)
	if err != nil {
		return nil, err
	}

	err = generateReduce(stmt, &doc)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

func generateMap(stmt *ast.CreateIndexStatement, doc *designdoc) error {

	buf := new(bytes.Buffer)

	fmt.Fprintln(buf, templStart)
	fmt.Fprintln(buf, templFunctions)

	keylist := new(bytes.Buffer)
	for idx, expr := range stmt.On {

		walker := NewWalker()
		_, err := walker.Visit(expr)
		if err != nil {
			panic(err)
		}

		jvar := fmt.Sprintf("key%v", idx+1)
		line := strings.Replace(templExpr, "$var", jvar, -1)
		line = strings.Replace(line, "$path", walker.JS(), -1)
		fmt.Fprint(buf, line)

		if idx > 0 {
			fmt.Fprint(keylist, ", ")
		}
		fmt.Fprint(keylist, jvar)
	}

	line := strings.Replace(templKey, "$keylist", keylist.String(), -1)
	fmt.Fprint(buf, line)
	fmt.Fprint(buf, templEmit)
	fmt.Fprint(buf, templEnd)
	doc.mapfn = buf.String()

	fmt.Println(doc.mapfn)
	return nil
}

func generateReduce(stmt *ast.CreateIndexStatement, doc *designdoc) error {
	// TODO
	doc.reducefn = ""
	return nil
}

func (idx *ViewIndex) putDesignDoc() error {
	bucket, err := getBucketForIndex(idx)
	if err != nil {
		return err
	}

	var view couchbase.ViewDefinition
	view.Map = idx.ddoc.mapfn

	var put DDocIndex
	put.Views = make(map[string]couchbase.ViewDefinition)
	put.Views[idx.ViewName()] = view
	put.DDLChecksum = idx.createChecksum()
	
	if err := bucket.PutDDoc(idx.DDocName(), &put); err != nil {
		return err
	}

	err = idx.checkDesignDoc()
	if err != nil {
		return api.DDocCreateFailed
	}

	fmt.Println("Created view:", idx.Name())
	return nil
}

func (idx *ViewIndex) createChecksum() int {
	mapSum := crc32.ChecksumIEEE([]byte(idx.ddoc.mapfn))
	reduceSum := crc32.ChecksumIEEE([]byte(idx.ddoc.reducefn))
	return int(mapSum + reduceSum)
}

func (idx *ViewIndex) verifyChecksum(actual int) bool {
	expected := idx.createChecksum() 
	return (expected == actual)
}

func (idx *ViewIndex) checkDesignDoc() error {
	bucket, err := getBucketForIndex(idx)
	if err != nil {
		return err
	}

	var ddoc DDocIndex
	if err := bucket.GetDDoc(idx.DDocName(), &ddoc); err != nil {
		return err
	}

	if !idx.verifyChecksum(ddoc.DDLChecksum) {
		return api.DDocChanged
	}
	
	return nil
}

func (idx *ViewIndex) DropViewIndex() error {
	bucket, err := getBucketForIndex(idx)
	if err != nil {
		return err
	}

	if err := bucket.DeleteDDoc(idx.DDocName()); err != nil {
		return err
	}

	fmt.Println("Dropped", idx.Name())

	return nil
}

func (idx *ViewIndex) DDocName() string {
	return "query_" + idx.Name()
}

func (idx *ViewIndex) ViewName() string {
	return "autogen"
}

func (this *ViewIndex) Name() string {
	return this.defn.Name
}

func (this *ViewIndex) Defn() *ast.CreateIndexStatement {
	return this.defn
}

func (this *ViewIndex) Type() api.IndexType {
	return api.View
}
