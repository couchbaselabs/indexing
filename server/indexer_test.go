package server

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/llrb"
	"os"
	"path/filepath"
	"testing"
)

func TestTrycreate(t *testing.T) {
	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))

	indexer := &IndexCatalog{
		DefaultLimit: DEFAULT_LIMIT,
		Datadir:      datadir,
		File:         filepath.Join(datadir, CATALOGFILE),
	}
	if err := indexer.tryCreate(); err != nil {
		t.Error("tryCreate failed:", err)
	}
	if _, err := os.Stat(indexer.File); err != nil {
		t.Error("tryCreate did not create catalog file:", err)
	}
	indexer.Purge()
}

func TestCodec(t *testing.T) {
	var indexer *IndexCatalog
	var err error
	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))

	if indexer, err = NewIndexCatalog(datadir); err != nil {
		t.Error("Cannot create index catalog file")
	}
	if len(indexer.Indexes) != 0 {
		t.Error("Indexes must be zero")
	}
	indexer.saveCatalog()
	if len(indexer.Indexes) != 0 {
		t.Error("Indexes must be zero")
	}
	indexer.loadCatalog()
	indexer.Purge()
}

func TestCatalog(t *testing.T) {
	var indexer, indexer_ *IndexCatalog
	var indexinfo api.IndexInfo
	var servUuid string
	var err error

	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))
	if indexer_, err := NewIndexCatalog(datadir); err != nil {
		t.Fatal("Cannot open index catalog file")
	} else {
		indexer = indexer_
	}

	indexinfo = api.IndexInfo{
		Name:       "test",
		Using:      api.Llrb,
		CreateStmt: `CREATE INDEX emailidx ON users (age+10)`,
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
		Expression: "",
	}

	// Test Create()
	if servUuid, indexinfo, err = indexer.Create(indexinfo); err != nil {
		t.Error("Cannot create index", indexinfo)
	} else if servUuid == "" {
		t.Error("server uuid is null")
	} else if indexinfo.Index == nil {
		t.Error("index.Index is nil after create index")
	} else if indexinfo.Uuid == "" {
		t.Error("index uuid is null")
	} else if len(indexer.Indexes) != 1 {
		t.Error("Indexes must have one index")
	}
	// Reload the indexes from catalog
	if indexer_, err = NewIndexCatalog(datadir); err != nil {
		panic(err)
		t.Fatal("Cannot re-open index catalog file", err)
	} else if len(indexer.Indexes) != 1 {
		t.Error("Indexes must have one index")
	} else {
		indexinfo := indexer_.Indexes[indexinfo.Uuid]
		if _, ok := indexinfo.Index.(*llrb.IndexLLRB); ok == false {
			t.Error("Cannot load back the index")
		} else {
			indexer = indexer_
		}
	}

	// Test Index()
	if indexinfo, err = indexer.Index(indexinfo.Uuid); err != nil {
		t.Error("Index() returned", err)
	} else if indexinfo.Name != indexinfo.Name {
		t.Error("Index() api fails", indexinfo)
	}

	// Test List()
	if _, ls, err := indexer.List(""); err != nil {
		t.Error(err)
	} else {
		if len(ls) != 1 {
			t.Error("List returns", len(ls))
		}
		if ls[0].CreateStmt != indexinfo.CreateStmt {
			t.Error("List returns create statement", ls[0].CreateStmt)
		}
	}

	// Test Drop()
	if _, err := indexer.Drop(indexinfo.Uuid); err != nil {
		t.Error("Drop() returned", err)
	}
	// Reload the indexes from catalog
	if indexer_, err := NewIndexCatalog(datadir); err != nil {
		t.Fatal("Cannot re-open index catalog file")
	} else if len(indexer.Indexes) != 0 {
		t.Error("Indexes must have one index")
	} else {
		indexer = indexer_
	}

	// Test Purge()
	indexer.Purge()
	if _, err := os.Stat(indexer.File); err == nil {
		t.Error("Purge did not delete the catalog file")
	}
}
