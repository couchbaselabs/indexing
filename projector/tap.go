package main

import (
    "github.com/couchbaselabs/go-couchbase"
    "github.com/prataprc/goupr"
    "github.com/couchbaselabs/indexing/api"
    mc "github.com/dustin/gomemcached/client"
    "log"
)

type Feed struct {
    streams   *TapStreams
    bucket    *couchbase.Bucket
    tapfeed   *couchbase.TapFeed
    indexinfo *api.IndexInfo
}

type TapStreams struct {
    eventch    chan *goupr.StreamEvent
    pool       couchbase.Pool
    indexinfos []api.IndexInfo
    feeds      map[string]*Feed
}

var tapop2type = map[mc.TapOpcode]string{
    mc.TapMutation: "INSERT",
    mc.TapDeletion: "DELETE",
}

func NewTapStreams(p couchbase.Pool, iinfos []api.IndexInfo,
    eventch chan *goupr.StreamEvent) *TapStreams {

    return &TapStreams{
        eventch: eventch,
        pool: p,
        indexinfos: iinfos,
        feeds: make(map[string]*Feed),
    }
}

func (streams *TapStreams) UpdateIndexInfos(iinfos []api.IndexInfo) {
    streams.indexinfos = iinfos
}

func (streams *TapStreams) OpenStreams() {
    var bucket *couchbase.Bucket
    var err error

    for _, indexinfo := range streams.indexinfos {
        if streams.feeds[indexinfo.Bucket] != nil {
            continue
        }
        if bucket, err = streams.pool.GetBucket(indexinfo.Bucket); err != nil {
            panic(err)
        }
        args := mc.TapArguments{
            Dump:       false,
            SupportAck: false,
            KeysOnly:   false,
            Checkpoint: true,
            ClientName: "",
        }
        if tapfeed, err := bucket.StartTapFeed(&args); err != nil {
            panic(err)
        } else {
            feed := &Feed{
                streams: streams,
                bucket: bucket,
                tapfeed: tapfeed,
                indexinfo: &indexinfo,
            }
            streams.feeds[indexinfo.Bucket] = feed
            go runFeed(feed)
        }
    }
    return
}

func runFeed(feed *Feed) {
    bucket := feed.indexinfo.Bucket
    log.Println("feed for bucket", bucket, "...")
    for {
        if event, ok := <-feed.tapfeed.C; ok {
            op := event.Opcode
            if op == mc.TapMutation || op == mc.TapDeletion {
                feed.streams.eventch <- &goupr.StreamEvent{
                    Bucket: bucket,
                    Opstr: tapop2type[op],
                    Key: event.Key,
                    Value: event.Value,
                }
            }
        } else {
            log.Println("closing tap feed for", bucket)
            break
        }
    }
}

func (streams *TapStreams) CloseStreams() {
    for _, f := range streams.feeds {
        f.bucket.Close()
        f.tapfeed.Close()
    }
}
