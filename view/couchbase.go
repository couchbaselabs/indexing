package view

import (
	"github.com/couchbaselabs/go-couchbase"
)

var buckets map[string]*couchbase.Bucket = make(map[string]*couchbase.Bucket)

func getBucketForIndex(idx *ViewIndex) (*couchbase.Bucket, error) {

	if cached := buckets[idx.url]; cached != nil {
		return cached, nil
	}

	cb, err := couchbase.Connect(idx.url)
	if err != nil {
		return nil, err
	}

	pool, err := cb.GetPool("default")
	if err != nil {
		return nil, err
	}

	bucket, err := pool.GetBucket(idx.defn.Bucket)
	if err != nil {
		return nil, err
	}

	buckets[idx.url] = bucket
	return bucket, nil
}
