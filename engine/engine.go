package engine

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/view"
	"github.com/couchbaselabs/tuqtng/ast"
)

func GetEngine() api.Indexer {
	return theEngine
}

func (this *engine) Create(stmt *ast.CreateIndexStatement) error {
	defer this.save()

	if _, present := this.indexes[stmt.Name]; present {
		return api.DuplicateIndex
	}

	// we'll have more types here
	switch stmt.View {
	case true:
		var inst api.Accesser = view.NewViewIndex(stmt)
		this.indexes[stmt.Name] = inst
		return nil

	case false:
		var inst api.Accesser = &TestIndexInstance{iname: stmt.Name, idefn: stmt, itype: api.View}
		this.indexes[stmt.Name] = inst
		return nil
	}

	return api.NoSuchType
}

func (this *engine) Drop(name string) error {
	defer this.save()

	inst := this.indexes[name]
	if inst == nil {
		return api.NoSuchIndex
	}

	delete(this.indexes, name)
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

func (this *engine) Index(name string) api.Accesser {
	return this.indexes[name]
}

type engine struct {
	indexes map[string]api.Accesser
	saves   chan int
}

type TestIndexInstance struct {
	iname string
	itype api.IndexType
	idefn *ast.CreateIndexStatement
}

func (this *TestIndexInstance) Name() string {
	return this.iname
}

func (this *TestIndexInstance) Defn() *ast.CreateIndexStatement {
	return this.idefn
}

func (this *TestIndexInstance) Type() api.IndexType {
	return this.itype
}

var theEngine api.Indexer = newEngine()

func newEngine() api.Indexer {
	inst := new(engine)
	inst.saves = make(chan int)
	inst.indexes = make(map[string]api.Accesser)

	inst.load()
	go inst.saver()
	return inst
}

func (this *engine) save() {
	this.saves <- 1
}

func (this *engine) saver() {
	for {
		<-this.saves
		// TODO
	}
}

func (this *engine) load() {
	// TODO
}
