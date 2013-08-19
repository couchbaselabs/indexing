package engine

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"os"
	"encoding/gob"
)

const indexdir = "index/"


func GetEngine() api.Indexer {
	return theEngine
}

type engine struct {
	indexes []api.AccessPath
	saves chan int
}

func (eng *engine) Create(statement ast.Statement) (api.AccessPath, error) {
	defer eng.save()
	// switch (ast.View.Type)
	inst := ViewInst{IdxInst{IName: "hello", Itype: api.View}}
	eng.indexes = append (eng.indexes, &inst)
	return &inst, nil
	// end switch
}

func (eng *engine) Drop(statement ast.Statement) error {
	defer eng.save()
	return nil
}
	
func (eng *engine) Instances() []api.AccessPath {
	return eng.indexes
}

var theEngine api.Indexer = newEngine()

func newEngine() api.Indexer {
	inst := new(engine)
	inst.saves = make(chan int)
	inst.load()
	go inst.saver()
	return inst
}

func (eng *engine) save() {
	eng.saves <- 1
}

func (eng *engine) saver() {
	for {
		<- eng.saves
		os.Mkdir(indexdir, 0700)
		handle, err := os.OpenFile(indexdir + "engine.gob", os.O_TRUNC | os.O_CREATE | os.O_WRONLY, 0600)
		if err != nil {
			panic(err)
		}
	    enc := gob.NewEncoder(handle)
	    gob.Register(*new(ViewInst))
	    if err := enc.Encode(eng.indexes); err != nil {
	    	panic(err)
	    }
		handle.Close()
	 }
}

func (eng *engine) load() {
  handle, err := os.OpenFile(indexdir + "engine.gob", os.O_RDONLY, 0600)
  if os.IsNotExist(err) {
    return
  }
  if err != nil {
  	panic(err)
  }
  defer handle.Close()
  dec := gob.NewDecoder(handle)
  if err := dec.Decode(eng.indexes); err != nil {
  	panic(err)
  }
}

type IdxInst struct {
	IName string
	Itype api.IndexType
}

type ViewInst struct {
	IdxInst
}

func (ii *IdxInst) Name() string {
	return ii.IName
}
	
func (ii *IdxInst) Type() api.IndexType {
	return ii.Itype
}
