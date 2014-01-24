package main

import (
    "fmt"
    "github.com/couchbaselabs/go-couchbase"
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
}

func NewUprStreams(c *couchbase.Client, p *couchbase.Pool,
    eventch chan *goupr.UprEvent) *UprStreams {

    return &UprStreams{
        client:  c,
        pool:    p,
        buckets: make(map[string]*couchbase.Bucket),
        feeds:   make(map[string]*goupr.UprFeed),
        eventch: eventch,
    }
}

func (streams *UprStreams) OpenStreams(buckets []string,
    bucketMap BucketVBVector) {

    var err error
    var pool couchbase.Pool

    // Refresh the pool to get any new buckets created on the server.
    pool, err = streams.client.GetPool("default")
    if err != nil {
        log.Println("ERROR: Unable to refresh the pool `default`")
        return
    }
    streams.pool = &pool

    for _, bname := range buckets {
        log.Println("Opening streams for bucket", bname)
        b, _ := streams.pool.GetBucket(bname)
        streams.buckets[bname] = b

        name := fmt.Sprintf("%v", time.Now().UnixNano())
        feed, err := goupr.StartUprFeed(b, name, nil)
        if err != nil {
            log.Println(err)
        }
        streams.feeds[bname] = feed
        go streams.getEvents(b, feed)
    }
    return
}

func (streams *UprStreams) getEvents(b *couchbase.Bucket, feed *goupr.UprFeed) {
    for {
        e, ok := <-feed.C
        if ok {
            streams.eventch <- &e
        } else {
            return
        }
    }
}

func (streams *UprStreams) CloseStreams() {
    log.Println("Closing feeds ...")
    for bname, bucket := range streams.buckets {
        bucket.Close()
        streams.feeds[bname].Close()
    }
}
