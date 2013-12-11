package catalog

import (
	"github.com/couchbaselabs/indexing/api"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

//FIXME Change from t.Error to t.Errorf
func TestTrycreate(t *testing.T) {
	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))

	c := &catalog{
		datadir:      datadir,
		file:         filepath.Join(datadir, CATALOGFILE),
	}
	if err := c.tryCreate(); err != nil {
		t.Error("tryCreate failed:", err)
	}
	if _, err := os.Stat(c.file); err != nil {
		t.Error("tryCreate did not create catalog file:", err)
	}
	c.Purge()
}

//FIXME Change from t.Error to t.Errorf
func TestCodec(t *testing.T) {
	var c *catalog
	var err error
	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))

	if c, err = NewIndexCatalog(datadir); err != nil {
		t.Error("Cannot create index catalog file")
	}
	if len(c.indexes) != 0 {
		t.Error("Indexes must be zero")
	}
	c.saveCatalog()
	if len(c.indexes) != 0 {
		t.Error("Indexes must be zero")
	}
	c.loadCatalog()
	c.Purge()
}

//FIXME Change from t.Error to t.Errorf
func TestCatalog(t *testing.T) {
	var c, c_ *catalog
	var indexinfo api.IndexInfo
	var servUuid string
	var err error

	datadir := "./"
	os.Remove(filepath.Join(datadir, CATALOGFILE))
	if c_, err := NewIndexCatalog(datadir); err != nil {
		t.Fatal("Cannot open index catalog file")
	} else {
		c = c_
	}

	indexinfo = api.IndexInfo{
		Name:       "test",
		Using:      api.Llrb,
		OnExprList: []string{`{"type":"property","path":"age"}`},
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
	}

	// Test Create()
	if servUuid, err = c.Create(indexinfo); err != nil {
		t.Error("Cannot create index", indexinfo)
	} else if servUuid == "" {
		t.Error("server uuid is null")
	} else if len(c.indexes) != 1 {
		t.Error("Indexes must have one index")
	}
	
	if err = c.Exists(indexinfo.Name, indexinfo.Bucket); err == nil {
		t.Error("Duplicate Index should not be allowed")
	}
	// Reload the indexes from catalog
	if c_, err = NewIndexCatalog(datadir); err != nil {
		t.Fatal("Cannot re-open index catalog file", err)
	} else if len(c_.indexes) != 1 {
		t.Error("Indexes must have one index")
	} else if !reflect.DeepEqual(c.indexes, c_.indexes) {
		t.Error("Cannot load back the index")
	} else {
			c = c_
	}

	// Test Index()
	if indexinfo, err = c.Index(indexinfo.Uuid); err != nil {
		t.Error("Index() returned", err)
	} else if indexinfo.Name != indexinfo.Name {
		t.Error("Index() api fails", indexinfo)
	}

	// Test List()
	if _, ls, err := c.List(""); err != nil {
		t.Error(err)
	} else {
		if len(ls) != 1 {
			t.Error("List returns", len(ls))
		}
		if !reflect.DeepEqual(ls[0].OnExprList, indexinfo.OnExprList) {
			t.Error("List returns create statement", ls[0].OnExprList)
		}
	}

	// Test Drop()
	if _, err := c.Drop(indexinfo.Uuid); err != nil {
		t.Error("Drop() returned", err)
	}
	// Reload the indexes from catalog
	if c_, err := NewIndexCatalog(datadir); err != nil {
		t.Fatal("Cannot re-open index catalog file")
	} else if len(c_.indexes) != 0 {
		t.Error("Indexes must have one index")
	} else {
		c = c_
	}

	// Test Purge()
	c.Purge()
	if _, err := os.Stat(c.file); err == nil {
		t.Error("Purge did not delete the catalog file")
	}
}
