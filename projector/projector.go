package main

import (
	"flag"
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	imclient "github.com/couchbaselabs/indexing/index_manager/client"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/goupr"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"
)

// TODO:
// [1] the node in which router runs will have to be mentioned in indexinfo
//     structure. once that is available change the projector accordingly

// exponential backoff while connection retry.
const initialRetryInterval = 1 * time.Second
const maximumRetryInterval = 30 * time.Second
const maxOutstandingDone = 10000

const (
	GETSEQUENCE_VECTOR string = "MutationManager.GetSequenceVectors"
	PROCESS_1MUTATION  string = "MutationManager.ProcessSingleMutation"
)

var options struct {
	kvhost string
	imhost string
	inhost string // TODO: [1]
	count  int
	proto  string
}

type streamer interface {
	openStreams(map[string][]uint64 /*bucket sequence*/) error
	closeStreams()
}

type projectorInfo struct {
	serverUuid  string
	imanager    *imclient.RestClient
	indexinfos  map[string]*api.IndexInfo // uuid is the index key
	indexers    map[string]*rpc.Client    // uuid is the index key
	bvb         map[string][]uint64       // bucket-name is the index key
	expressions map[string][]ast.Expression
}

func argParse() {
	flag.StringVar(&options.kvhost, "kvhost", "localhost:11211",
		"Port to connect to kv-cluster")
	flag.StringVar(&options.inhost, "inhost", "localhost:8096",
		"Port to connect to indexer node") // TODO [1]
	flag.StringVar(&options.imhost, "imhost", "localhost:8094",
		"Port to connect to index-manager node")
	flag.StringVar(&options.proto, "proto", "upr",
		"Use either `tap` or `upr`")
	flag.Parse()
}

func main() {
	argParse()
	// Couchbase client, pool and default bucket
	couch, err := couchbase.Connect("http://" + options.kvhost)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	eventch := make(chan *goupr.UprEvent)
	streams := NewUprStreams(&couch, &pool, eventch)

	for {
		p := &projectorInfo{}
		p.getMetaData()
		if err := streams.openStreams(p.bvb); err == nil {
			notifych := make(chan string, 1)
			go p.waitNotify(notifych)
			p.loop(notifych, eventch)
			streams.closeStreams()
			p.close()
		} else {
			log.Println("Error openStreams():", err)
		}
		log.Println("Projector restarting ...")
	}
}

func (p *projectorInfo) getMetaData() (err error) {
	var rpcconn net.Conn
	var returnMap api.IndexSequenceMap
	var indexinfos []api.IndexInfo
	var ex ast.Expression

	tryConnection(func() bool {
		p.imanager = imclient.NewRestClient("http://" + options.imhost)
		if p.serverUuid, indexinfos, err = p.imanager.List(""); err != nil {
			log.Println("Error getting list:", err)
			return false
		}
		// Create a map of indexinfos
		p.indexinfos = make(map[string]*api.IndexInfo)
		for i, ii := range indexinfos {
			p.indexinfos[ii.Uuid] = &indexinfos[i]
		}
		// Create a map of indexer clients
		p.indexers = make(map[string]*rpc.Client)
		p.bvb = make(map[string][]uint64)
		p.expressions = make(map[string][]ast.Expression)
		for uuid, ii := range p.indexinfos {
			// url := ii.RouterNode
			url := options.inhost // TODO [1]
			if rpcconn, err = net.Dial("tcp", url); err != nil {
				log.Printf("error connecting with indexer %v: %v\n", url, err)
				return false
			}
			c := jsonrpc.NewClient(rpcconn)
			p.indexers[uuid] = c
			// Get sequence vector for each index
			indexList := api.IndexList{uuid}
			if err = c.Call(GETSEQUENCE_VECTOR, &indexList, &returnMap); err != nil {
				log.Printf("getting sequence vector %v: %v\n", url, err)
				return false
			}
			// Build sequence vector per bucket based on the collection of indexes
			// defined on the bucket.
			for uuid, vector := range returnMap {
				bname := p.indexinfos[uuid].Bucket
				for vb, seqno := range vector {
					if p.bvb[bname] == nil {
						p.bvb[bname] = make([]uint64, api.MAX_VBUCKETS)
					}
					if p.bvb[bname][vb] == 0 || p.bvb[bname][vb] > seqno {
						p.bvb[bname][vb] = seqno
					}
				}
			}

			astexprs := make([]ast.Expression, 0)
			for _, expr := range ii.OnExprList {
				ex, err = ast.UnmarshalExpression([]byte(expr))
				if err != nil {
					log.Printf("unmarshal error: %v", err)
					return false
				}
				astexprs = append(astexprs, ex)
			}
			p.expressions[uuid] = astexprs
		}
		log.Printf(
			"Got %v indexes in %v buckets\n", len(p.indexinfos), len(p.bvb))
		return true
	})
	return
}

func (p *projectorInfo) waitNotify(notifych chan string) {
	var err error
	if _, p.serverUuid, err = p.imanager.Notify(p.serverUuid); err != nil {
		log.Println("Index manager closed connection:", err)
		close(notifych)
	}
	notifych <- p.serverUuid
}

func (p *projectorInfo) close() {
	for _, c := range p.indexers {
		c.Close()
	}
}

func (p *projectorInfo) loop(notifych chan string, eventch chan *goupr.UprEvent) {
	var r bool
	donech := make(chan *rpc.Call, maxOutstandingDone)
loop:
	for {
		select {
		case serverUuid, ok := <-notifych:
			if ok {
				log.Println("Notification received, serverUuid:", serverUuid)
			}
			break loop
		case call := <-donech:
			if *call.Reply.(*bool) == false {
				m := call.Args.(api.Mutation)
				log.Println(
					"%v failed (%v, %v, %v): %v",
					m.Indexid, m.Docid, m.Seqno, call.Error)
			}
		case e := <-eventch:
			for uuid, astexprs := range p.expressions {
				ii := p.indexinfos[uuid]
				bucket, idxuuid := ii.Bucket, ii.Uuid
				if e.Bucket != bucket {
					continue
				}
				m := api.Mutation{
					Type:    api.UprEventName(e.Opstr),
					Indexid: idxuuid,
					Docid:   string(e.Key),
					Vbucket: e.Vbucket,
					Seqno:   e.Seqno,
				}
				if ii.IsPrimary && m.Type == api.INSERT {
					m.SecondaryKey = [][]byte{e.Key}
				} else if m.Type == api.INSERT {
					m.SecondaryKey = evaluate(e.Value, astexprs)
				}
				log.Println(e.Opstr, e.Seqno, idxuuid[:8], bucket, m.Docid, fmtSKey(m.SecondaryKey))
				c := p.indexers[uuid]
				c.Go(PROCESS_1MUTATION, m, &r, donech)
			}
		}
	}
}

func evaluate(value []byte, astexprs []ast.Expression) [][]byte {
	secKey := make([][]byte, 0)
	for _, expr := range astexprs {
		key, err := expr.Evaluate(dparval.NewValueFromBytes([]byte(value)))
		if err != nil {
			log.Println(err)
			secKey = append(secKey, []byte{})
		} else {
			secKey = append(secKey, key.Bytes())
		}
	}
	return secKey
}

func tryConnection(fn func() bool) {
	retryInterval := initialRetryInterval
	for {
		ok := fn()
		if ok {
			return
		}
		log.Printf("Retrying after %v seconds ...\n", retryInterval)
		<-time.After(retryInterval)
		if retryInterval *= 2; retryInterval > maximumRetryInterval {
			retryInterval = maximumRetryInterval
		}
	}
}

func fmtSKey(keys [][]byte) []string {
	ss := make([]string, 0)
	for _, bs := range keys {
		ss = append(ss, string(bs))
	}
	return ss
}
