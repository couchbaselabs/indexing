package main

import (
	"fmt"
	"github.com/prataprc/go-couchbase"
	"github.com/prataprc/goupr"
	"log"
	//"strings"
	"time"
)

type UprStreams struct {
	client  *couchbase.Client
	pool    *couchbase.Pool
	buckets map[string]*couchbase.Bucket // [bucketname]*couchbase.Bucket
	feeds   map[string]*goupr.UprFeed
	eventch chan *goupr.UprEvent
	quit    chan bool
}

func NewUprStreams(c *couchbase.Client, p *couchbase.Pool,
	eventch chan *goupr.UprEvent) *UprStreams {

	return &UprStreams{
		client:  c,
		pool:    p,
		buckets: make(map[string]*couchbase.Bucket),
		feeds:   make(map[string]*goupr.UprFeed),
		eventch: eventch,
		quit:    make(chan bool),
	}
}

func (streams *UprStreams) openStreams(bvb map[string][]uint64) (err error) {
	var pool couchbase.Pool
	var b *couchbase.Bucket
	var flogs []goupr.FailoverLog
	var feed *goupr.UprFeed

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
		if flogs, err = goupr.GetFailoverLogs(b, name); err != nil {
			break
		}
		uprstreams := makeUprStream(seqVector, flogs)
		if feed, err = goupr.StartUprFeed(b, name, uprstreams); err != nil {
			break
		}
		streams.feeds[bname] = feed
		go streams.getEvents(b, feed)
	}
	return
}

func (streams *UprStreams) getEvents(b *couchbase.Bucket, feed *goupr.UprFeed) {
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

func makeUprStream(seqVector []uint64, flogs []goupr.FailoverLog) map[uint16]*goupr.UprStream {
	uprstreams := make(map[uint16]*goupr.UprStream)
	for vbno, flog := range flogs {
		vb := uint16(vbno)
		uprstream := &goupr.UprStream{
			Vbucket:  vb,
			Vuuid:    flog[0][0],
			Startseq: seqVector[vb],
			Endseq:   0xFFFFFFFFFFFFFFFF,
		}
		uprstreams[vb] = uprstream
	}
	return uprstreams
}
