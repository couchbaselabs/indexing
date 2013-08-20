package engine

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/tuqtng/ast"
	"errors"
)

func GetEngine() api.Indexer {
	return theEngine
}

func (eng *engine) Create(stmt *ast.CreateIndexStatement) error {
	defer eng.save()
	if _, present := eng.indexes[stmt.Name]; present {
		return errors.New("Index by the same name already exists: " + stmt.Name)
	}
	inst := api.IndexInstance{Name: stmt.Name, Definition: stmt, Type: api.View}
	eng.indexes[stmt.Name] = &inst
	return nil
}

func (eng *engine) Drop(name string) error {
	defer eng.save()
	inst := eng.indexes[name]
	if inst == nil {
		return errors.New("Index by the name does not exist: " + name)
	}
	delete(eng.indexes, name)
	return nil
}
	
func (eng *engine) Indexes() []string {
	rv := make([]string, len(eng.indexes))
	pos := 0
	for name := range eng.indexes {
		rv[pos] = name
		pos++
	}
	return rv
}

func (eng *engine) Index(name string) *api.IndexInstance {
	return eng.indexes[name]
}

type engine struct {
	indexes map[string]*api.IndexInstance
	saves chan int
}

var theEngine api.Indexer = newEngine()

func newEngine() api.Indexer {
	inst := new(engine)
	inst.saves = make(chan int)
	inst.indexes = make(map[string]*api.IndexInstance)

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
		// TODO
	 }
}

func (eng *engine) load() {
	// TODO
}
