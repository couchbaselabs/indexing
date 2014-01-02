package main

import (
    "github.com/couchbaselabs/go-couchbase"
    "github.com/couchbaselabs/indexing/api"
    "github.com/prataprc/goupr"
    "log"
    "strings"
    "fmt"
)

type UprStreams struct {
    pool       couchbase.Pool
    buckets    map[string]*couchbase.Bucket // [bucketname]*couchbase.Bucket
    // [bucketname]{hostname : *goupr.Client}
    clients    map[string]map[string]*goupr.Client
    eventch    chan *goupr.StreamEvent
}

type UprFeed struct { // per bucket, per node, per vbucket feed (aka stream)
    streams   *UprStreams
    bucket    *couchbase.Bucket
    node      string
    uprstream *goupr.Stream
}

func NewUprStreams(p couchbase.Pool, eventch chan *goupr.StreamEvent) *UprStreams {
    return &UprStreams{
        pool:       p,
        buckets:    make(map[string]*couchbase.Bucket),
        clients:    make(map[string]map[string]*goupr.Client),
        eventch:    eventch,
    }
}

func (streams *UprStreams) UpdateIndexInfos(iinfos []api.IndexInfo) {
    // We don't need indexinfos
}

func (streams *UprStreams) OpenStreams() {
    var err error
    for name, _ := range streams.pool.BucketMap {
        b, _ := streams.pool.GetBucket(name)
        streams.buckets[name] = b
        if streams.clients[name], err = streams.connectNodes(b); err != nil {
            log.Println(err)
        } else {
            vbmaps := b.VBSMJson.VBucketMap
            opaqCount := uint32(1)
            for vbucket, _ := range vbmaps {
                openFeed(b, streams.clients[name], uint16(vbucket), opaqCount)
                opaqCount += 1
                //break // TODO: REmove this
            }
        }
        //break // TODO: REmove this
    }
    return
}

func (streams *UprStreams) CloseStreams() {
    log.Println("Closing feeds ...")
    for _, clientmap := range streams.clients {
        for _, client := range clientmap {
            client.Close()
        }
    }
    for _, bucket := range streams.buckets {
        bucket.Close()
    }
}

func (streams *UprStreams) connectNodes(
    bucket *couchbase.Bucket) (map[string]*goupr.Client, error) {

    servers := bucket.VBSMJson.ServerList
    nodes := make(map[string]*goupr.Client)
    for _, hostname := range servers {
        client := goupr.NewClient(bucket, streams.eventch)
        name := strings.Join([]string{"indexer", bucket.Name, hostname}, "/")
        if err := client.Connect(hostname, name, false); err != nil {
            return nil, fmt.Errorf("Not able to connect with %v: %v", name, err)
        } else {
            log.Printf("Connected to %v as %v", hostname, name)
        }
        client.AutoRestart(true)
        nodes[hostname] = client
    }
    return nodes, nil
}

func openFeed(b *couchbase.Bucket, nodes map[string]*goupr.Client,
    vb uint16, opaque uint32) {

    servers, vbmaps := b.VBSMJson.ServerList, b.VBSMJson.VBucketMap

    // vbs_highseq_no := b.GetStats("vbucket-seqno")
    // key := fmt.Sprintf("vb_%v_high_seqno", vb)
    node := servers[vbmaps[int(vb)][0]]
    client := nodes[node]
    req := goupr.NewRequest(0, 0, opaque, vb)
    vuuid, flags := uint64(0), uint32(0)
    high, start, end := uint64(0), uint64(0), uint64(0xFFFFFFFFFFFFFFF)
    stream, _, err := client.UprStream(req, flags, start, end, vuuid, high)
    if err != nil {
        log.Println("Error opening feed for vbucket", vb, err)
    } else {
        log.Println("Opening feed for vbucket", vb)
        stream.AutoRestart(true)
    }
}
