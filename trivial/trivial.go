//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package trivial

import (
	"github.com/couchbaselabs/indexing/api"
	//  "github.com/petar/GoLLRB/llrb"
	"reflect"
)

type Trivial struct {
	//  tree llrb.LLRB
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

	switch reflect.TypeOf(op) {

	case reflect.TypeOf(t):
		return api.TraitInfo{
			Unique:     api.NonUnique,
			Order:      api.Asc,
			Accuracy:   api.Perfect,
			AvgTime:    api.Ologn,
			AvgSpace:   api.On,
			WorstTime:  api.Ologn,
			WorstSpace: api.On,
		}

	case reflect.TypeOf((*Trivial).Exists):
		return api.TraitInfo{
			Unique:     api.NonUnique,
			Order:      api.Asc,
			Accuracy:   api.Perfect,
			AvgTime:    api.Ologn,
			AvgSpace:   api.O1,
			WorstTime:  api.Ologn,
			WorstSpace: api.O1,
		}

	case reflect.TypeOf((*Trivial).Lookup):
		return api.TraitInfo{
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
