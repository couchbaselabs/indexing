package main

import (
	"flag"
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	imclient "github.com/couchbaselabs/indexing/index_manager/client"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/goupr"
	//memcached "github.com/dustin/gomemcached"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
)

var options struct {
	kvhost   string
	imhost   string
	inhost   string
	port     int
	userProd string
	projProd string
	seed     int
	count    int
	proto    string
}

// BucketVBVector maps bucket to a 2 dimensional array of
// [[vbcuket0, startSeq], [vbcuket1, startSeq], ..., [api.MAX_VBUCKETS, seq]]
type BucketVBVector map[string][]uint64

func argParse() {
	flag.StringVar(&options.kvhost, "kvhost", "localhost", "Port to connect")
	flag.StringVar(&options.imhost, "imhost", "localhost", "Port to connect")
	flag.StringVar(&options.inhost, "inhost", "localhost", "Port to connect")
	flag.IntVar(&options.port, "port", 11211, "Host to connect")
	flag.StringVar(&options.userProd, "userProd", "", "monster production for users")
	flag.StringVar(&options.projProd, "projProd", "", "monster production for project")
	flag.IntVar(&options.seed, "seed", 100, "seed for production")
	flag.IntVar(&options.count, "count", 10, "number of documents to generate")
	flag.StringVar(&options.proto, "proto", "tap", "Use either `tap` or `upr`")
	flag.Parse()
}

type Streamer interface {
	OpenStreams([]string, BucketVBVector)
	CloseStreams()
}

func main() {
	argParse()

	//mcdport := int(8091)
	//if options.port == 11212 {
	//    mcdport = 12000
	//}

	// Couchbase client, pool and default bucket
	url := "http://" + options.kvhost + ":" + strconv.Itoa(options.port)
	couch, err := couchbase.Connect(url)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	imURL := "http://" + options.imhost + ":" + strconv.Itoa(8094)
	imanager := imclient.NewRestClient(imURL)
	// nodes := imanager.Nodes()

	inURL := options.imhost + ":" + strconv.Itoa(8096)
	rpcconn, err := net.Dial("tcp", inURL)
	if err != nil {
		panic(err)
	}
	defer rpcconn.Close()
	c := jsonrpc.NewClient(rpcconn)

	eventch := make(chan *goupr.UprEvent)
	var streams Streamer
	switch options.proto {
	//case "tap":
	//    streams = NewTapStreams(&couch, &pool, eventch)
	case "upr":
		streams = NewUprStreams(&couch, &pool, eventch)
	}

	for {
		serverUuid, indexinfos, err := imanager.List("")
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("Got %v indexes", len(indexinfos))
		bucketMap := getSequenceVectors(c, indexinfos)
		streams.OpenStreams(indexBuckets(indexinfos), bucketMap)
		bucketexprs := parseExpression(indexinfos)
		notifych := make(chan string, 1)
		go waitNotify(imanager, serverUuid, notifych)
		loop(c, notifych, eventch, bucketexprs)
		streams.CloseStreams()
		log.Println("Projector restarting ...")
	}
}

func loop(c *rpc.Client, notifych chan string, eventch chan *goupr.UprEvent,
	bucketexprs map[*api.IndexInfo][]ast.Expression) {

Loop:
	for {
		select {
		case serverUuid := <-notifych:
			log.Println("Notification received, serverUuid:", serverUuid)
			break Loop
		case e := <-eventch:
			for indexinfo, astexprs := range bucketexprs {
				bucket, idxuuid := indexinfo.Bucket, indexinfo.Uuid
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
				if indexinfo.IsPrimary && e.Opstr == "INSERT" {
					m.SecondaryKey = [][]byte{e.Key}
				} else if e.Opstr == "INSERT" {
					m.SecondaryKey = evaluate(e.Value, astexprs)
				}
				log.Println("mutation recevied", e.Opstr, idxuuid, bucket,
					m.Docid, fmtSKey(m.SecondaryKey))
				var r bool
				err := c.Call("MutationManager.ProcessSingleMutation", m, &r)
				if err != nil {
					log.Println("ERROR: Mutation manager:", err)
				}
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

func parseExpression(indexinfos []api.IndexInfo) map[*api.IndexInfo][]ast.Expression {
	bucketexprs := make(map[*api.IndexInfo][]ast.Expression)
	for i, indexinfo := range indexinfos {
		astexprs := make([]ast.Expression, 0)
		for _, expr := range indexinfo.OnExprList {
			if ex, err := ast.UnmarshalExpression([]byte(expr)); err == nil {
				astexprs = append(astexprs, ex)
			} else {
				panic(err)
			}
		}
		bucketexprs[&indexinfos[i]] = astexprs
	}
	return bucketexprs
}

func waitNotify(imanager *imclient.RestClient, serverUuid string, notifych chan string) {
	if _, serverUuid, err := imanager.Notify(serverUuid); err != nil {
		panic(err)
	} else {
		notifych <- serverUuid
	}
}

func indexBuckets(indexinfos []api.IndexInfo) []string {
	buckets := make(map[string]bool)
	for _, indexinfo := range indexinfos {
		if buckets[indexinfo.Bucket] == false {
			buckets[indexinfo.Bucket] = true
		}
	}
	uniquebuckets := make([]string, 0)
	for bucket, _ := range buckets {
		uniquebuckets = append(uniquebuckets, bucket)
	}
	return uniquebuckets
}

func getSequenceVectors(c *rpc.Client,
	indexinfos []api.IndexInfo) BucketVBVector {

	var returnMap api.IndexSequenceMap
	indexMap := make(map[string]api.IndexInfo)
	// Argument to GetSequenceVectors()
	indexList := make([]string, 0)
	for _, ii := range indexinfos {
		indexList = append(indexList, ii.Uuid)
		indexMap[ii.Uuid] = ii
	}
	err := c.Call("MutationManager.GetSequenceVectors", &indexList, &returnMap)
	if err != nil {
		log.Println("ERROR: Mutation manager:", err)
	}
	bvb := make(BucketVBVector)
	for index_uuid, vector := range returnMap {
		log.Println(vector)
		bucket := indexMap[index_uuid].Bucket
		for vb, seqno := range vector {
			if bvb[bucket] == nil {
				bvb[bucket] = make([]uint64, api.MAX_VBUCKETS)
			}
			if bvb[bucket][vb] == 0 || bvb[bucket][vb] > seqno {
				bvb[bucket][vb] = seqno
			}
		}
	}
	log.Println(bvb)
	return bvb
}

func fmtSKey(keys [][]byte) []string {
	ss := make([]string, 0)
	for _, bs := range keys {
		ss = append(ss, string(bs))
	}
	return ss
}
