package leveldb

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/jmhodges/levigo"
	"log"
)

//FIXME try to use single leveldb object, rather than all the elements here
type LevelDBEngine struct {
	name    string
	options *levigo.Options
	ro      *levigo.ReadOptions
	wo      *levigo.WriteOptions
	c       *levigo.DB
	b       *levigo.DB
	trait   api.TraitInfo
}

func NewIndexEngine(name string) (engine api.Finder) {
	var err error
	if engine, err = Create(name); err != nil {
		log.Printf("Error Creating LevelDB Engine %v", err)
	}
	return engine
}

func OpenIndexEngine(name string) (engine api.Finder) {

	var err error
	if engine, err = Open(name); err != nil {
		log.Printf("Error Creating LevelDB Engine %v", err)
	}
	return engine
}
