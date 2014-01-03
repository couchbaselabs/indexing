package main

import (
    "github.com/couchbaselabs/go-couchbase"
    mc "github.com/dustin/gomemcached/client"
    "github.com/prataprc/goupr"
    "log"
)

type TapStreams struct {
    client  *couchbase.Client
    pool    *couchbase.Pool
    buckets map[string]*couchbase.Bucket // [bucketname]*couchbase.Bucket
    feeds   map[string]*couchbase.TapFeed // [bucketname]*couchbase.TapFeed
    eventch chan *goupr.StreamEvent
}

var tapop2type = map[mc.TapOpcode]string{
    mc.TapMutation: "INSERT",
    mc.TapDeletion: "DELETE",
}

func NewTapStreams(c *couchbase.Client, p *couchbase.Pool,
    eventch chan *goupr.StreamEvent) *TapStreams {

    return &TapStreams{
        client:     c,
        pool:       p,
        buckets:    make(map[string]*couchbase.Bucket),
        feeds:      make(map[string]*couchbase.TapFeed),
        eventch:    eventch,
    }
}

func (streams *TapStreams) OpenStreams(buckets []string) {
    var bucket *couchbase.Bucket
    var pool couchbase.Pool
    var err error

    pool, err = streams.client.GetPool("default")
    if err != nil {
        log.Println("ERROR: Unable to refresh the pool `default`")
        return
    }
    streams.pool = &pool
    for _, bname := range buckets {
        if bucket, err = streams.pool.GetBucket(bname); err != nil {
            log.Println("ERROR: failed to get bucket", bname, "err:", err)
            break
        }
        streams.buckets[bname] = bucket
        args := mc.TapArguments{
            Dump:       false,
            SupportAck: false,
            KeysOnly:   false,
            Checkpoint: true,
            ClientName: "",
        }
        if tapfeed, err := bucket.StartTapFeed(&args); err == nil {
            streams.feeds[bname] = tapfeed
            go runFeed(streams, bname, tapfeed)
        } else {
            log.Println("ERROR: failed to get feed for bucket", bname, "err:", err)
            break
        }
    }
    return
}

func runFeed(streams *TapStreams, b string, tapfeed *couchbase.TapFeed) {
    log.Println("feed for bucket", b, "...")
    for {
        if event, ok := <-tapfeed.C; ok {
            op := event.Opcode
            if op == mc.TapMutation || op == mc.TapDeletion {
                streams.eventch <- &goupr.StreamEvent{
                    Bucket: b,
                    Opstr:  tapop2type[op],
                    Key:    event.Key,
                    Value:  event.Value,
                }
            }
        } else {
            log.Println("Closing tap feed for", b)
            break
        }
    }
}

func (streams *TapStreams) CloseStreams() {
    log.Printf("Closing %v streams", len(streams.feeds))
    for _, bucket := range streams.buckets {
        bucket.Close()
    }
    for _, tapfeed := range streams.feeds {
        tapfeed.Close()
    }
}
