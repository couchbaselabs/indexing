// Copyright 2010 Petar Maymounkov. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package llrb

import (
	"github.com/couchbaselabs/indexing/api"
)

// GetHeight() returns a key in the tree with key @key, and it's height in the tree
func (t *LLRB) GetHeight(key api.Key) (result api.Key, depth int) {
	return t.getHeight(t.root, key)
}

func (t *LLRB) getHeight(h *Node, key api.Key) (api.Key, int) {
	if h == nil {
		return nil, 0
	}
	if less(key, h.Key) {
		result, depth := t.getHeight(h.Left, key)
		return result, depth + 1
	}
	if less(h.Key, key) {
		result, depth := t.getHeight(h.Right, key)
		return result, depth + 1
	}
	return h.Key, 0
}

// HeightStats() returns the average and standard deviation of the height
// of elements in the tree
func (t *LLRB) HeightStats() (avg, stddev float64) {
	av := &avgVar{}
	heightStats(t.root, 0, av)
	return av.GetAvg(), av.GetStdDev()
}

func heightStats(h *Node, d int, av *avgVar) {
	if h == nil {
		return
	}
	av.Add(float64(d))
	if h.Left != nil {
		heightStats(h.Left, d+1, av)
	}
	if h.Right != nil {
		heightStats(h.Right, d+1, av)
	}
}
