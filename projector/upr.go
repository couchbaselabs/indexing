//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/prataprc/go-couchbase"
	"log"
	"time"
)

type UprBucketFeed struct {
	bucket *couchbase.Bucket // [bucketname]*couchbase.Bucket
	feed   *couchbase.UprFeed
}

func NewUprStreams(b *couchbase.Bucket) *UprBucketFeed {
	return &UprBucketFeed{bucket: b}
}

func (bfeed *UprBucketFeed) openFeed(sv api.SequenceVector) (err error) {
	log.Println("Opening feed for bucket:", bfeed.bucket.Name)
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	//name := "index"
	flogs, err := bfeed.bucket.GetFailoverLogs(name)
	if err != nil {
		return
	}
	uprstreams := makeUprStream(sv, flogs)
	bfeed.feed, err = couchbase.StartUprFeed(bfeed.bucket, name, uprstreams)
	if err != nil {
		return
	}
	return
}

func (bfeed *UprBucketFeed) closeFeed() {
	log.Printf("Closing feed for %v ...\n", bfeed.bucket.Name)
	bfeed.feed.Close()
}

func makeUprStream(seqVector api.SequenceVector,
	flogs []couchbase.FailoverLog) map[uint16]*couchbase.UprStream {

	uprstreams := make(map[uint16]*couchbase.UprStream)
	for vbno, flog := range flogs {
		vb := uint16(vbno)
		vuuid, startSeq, highSeq := calculateVector(seqVector[vb], flog)
		uprstream := &couchbase.UprStream{
			Vbucket:  vb,
			Vuuid:    vuuid,
			Startseq: startSeq,
			Highseq:  highSeq,
			Endseq:   0xFFFFFFFFFFFFFFFF,
		}
		uprstreams[vb] = uprstream
	}
	return uprstreams
}

func calculateVector(
	lastSeq uint64, flog couchbase.FailoverLog) (uint64, uint64, uint64) {

	for _, log := range flog {
		if lastSeq >= log[1] {
			return log[0], lastSeq, log[1]
		}
	}
	return flog[0][0], flog[0][1], flog[0][1]
}
