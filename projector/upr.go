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
	"github.com/prataprc/go-couchbase"
	"log"
	//"strings"
	"time"
)

type UprStreams struct {
	client  *couchbase.Client
	pool    *couchbase.Pool
	buckets map[string]*couchbase.Bucket // [bucketname]*couchbase.Bucket
	feeds   map[string]*couchbase.UprFeed
	eventch chan *couchbase.UprEvent
	quit    chan bool
}

func NewUprStreams(c *couchbase.Client, p *couchbase.Pool,
	eventch chan *couchbase.UprEvent) *UprStreams {

	return &UprStreams{
		client:  c,
		pool:    p,
		buckets: make(map[string]*couchbase.Bucket),
		feeds:   make(map[string]*couchbase.UprFeed),
		eventch: eventch,
		quit:    make(chan bool),
	}
}

func (streams *UprStreams) openStreams(bvb map[string][]uint64) (err error) {
	var pool couchbase.Pool
	var b *couchbase.Bucket
	var flogs []couchbase.FailoverLog
	var feed *couchbase.UprFeed

	// Refresh the pool to get any new buckets created on the server.
	pool, err = streams.client.GetPool("default")
	if err != nil {
		return
	}
	streams.pool = &pool

	for bname, seqVector := range bvb {
		log.Println("Opening streams for bucket", bname)
		if b, err = streams.pool.GetBucket(bname); err != nil {
			break
		}
		streams.buckets[bname] = b
		name := fmt.Sprintf("%v", time.Now().UnixNano())
		if flogs, err = couchbase.GetFailoverLogs(b, name); err != nil {
			break
		}
		uprstreams := makeUprStream(seqVector, flogs)
		if feed, err = couchbase.StartUprFeed(b, name, uprstreams); err != nil {
			break
		}
		streams.feeds[bname] = feed
		go streams.getEvents(b, feed)
	}
	return
}

func (streams *UprStreams) getEvents(b *couchbase.Bucket, feed *couchbase.UprFeed) {
loop:
	for {
		select {
		case e, ok := <-feed.C:
			if ok {
				streams.eventch <- &e
			} else {
				break loop
			}
		case <-streams.quit:
			break loop
		}
	}
}

func (streams *UprStreams) closeStreams() {
	log.Println("Closing feeds ...")
	close(streams.quit)
	for bname, bucket := range streams.buckets {
		bucket.Close()
		streams.feeds[bname].Close()
	}
}

func makeUprStream(seqVector []uint64,
	flogs []couchbase.FailoverLog) map[uint16]*couchbase.UprStream {

	uprstreams := make(map[uint16]*couchbase.UprStream)
	for vbno, flog := range flogs {
		vb := uint16(vbno)
		uprstream := &couchbase.UprStream{
			Vbucket:  vb,
			Vuuid:    flog[0][0],
			Startseq: seqVector[vb],
			Endseq:   0xFFFFFFFFFFFFFFFF,
		}
		uprstreams[vb] = uprstream
	}
	return uprstreams
}
