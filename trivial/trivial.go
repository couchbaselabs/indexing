package trivial

import (
	"github.com/couchbaselabs/indexing/api"
//	"github.com/petar/GoLLRB/llrb"
	"reflect"
)

type Trivial struct {
//	tree llrb.LLRB
}

func (t *Trivial) Name() string {
	return "trivial"
}

func (t *Trivial) Lookup(key api.Key) chan api.Value {
	return nil
}

func (t *Trivial) Exists(key api.Key) bool {
	return false
}

func (t *Trivial) Traits(op interface{}) api.TraitInfo {

	switch (reflect.TypeOf(op)) {

		case reflect.TypeOf(t):
		return api.TraitInfo {
			Unique:     api.NonUnique,
			Order:      api.Asc,
			Accuracy:   api.Perfect,
			AvgTime:    api.Ologn,
			AvgSpace:   api.On,
			WorstTime:  api.Ologn,
			WorstSpace: api.On,
		}

		case reflect.TypeOf((*Trivial).Exists):
		return api.TraitInfo {
			Unique:     api.NonUnique,
			Order:      api.Asc,
			Accuracy:   api.Perfect,
			AvgTime:    api.Ologn,
			AvgSpace:   api.O1,
			WorstTime:  api.Ologn,
			WorstSpace: api.O1,
		}

		case reflect.TypeOf((*Trivial).Lookup):
			return api.TraitInfo {
				Unique:     api.NonUnique,
				Order:      api.Asc,
				Accuracy:   api.Perfect,
				AvgTime:    api.Ologn,
				AvgSpace:   api.Om,
				WorstTime:  api.Ologn,
				WorstSpace: api.Om,
			}

		default:
			panic("Unknown operator:" + reflect.TypeOf(op).Kind().String())
	}
}

