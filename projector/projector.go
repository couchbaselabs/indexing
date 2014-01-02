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
	//"net"
	"net/rpc"
	//"net/rpc/jsonrpc"
	"strconv"
)

var options struct {
	host     string
	port     int
	userProd string
	projProd string
	seed     int
	count    int
	proto    string
}

func argParse() {
	flag.StringVar(&options.host, "host", "localhost", "Port to connect")
	flag.IntVar(&options.port, "port", 11211, "Host to connect")
	flag.StringVar(&options.userProd, "userProd", "", "monster production for users")
	flag.StringVar(&options.projProd, "projProd", "", "monster production for project")
	flag.IntVar(&options.seed, "seed", 100, "seed for production")
	flag.IntVar(&options.count, "count", 10, "number of documents to generate")
	flag.StringVar(&options.proto, "proto", "tap", "Use either `tap` or `upr`")
	flag.Parse()
}

type Streamer interface {
	OpenStreams()
	CloseStreams()
	UpdateIndexInfos([]api.IndexInfo)
}

func main() {
	argParse()

	//mcdport := int(8091)
	//if options.port == 11212 {
	//    mcdport = 12000
	//}

	// Couchbase client, pool and default bucket
	url := "http://" + options.host + ":" + strconv.Itoa(options.port)
	couch, err := couchbase.Connect(url)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	imURL := "http://" + options.host + ":" + strconv.Itoa(8094)
	imanager := imclient.NewRestClient(imURL)
	// nodes := imanager.Nodes()

	// Start pumping user mutations if user-production file is available
	if options.userProd != "" {
		if users, err := pool.GetBucket("users"); err == nil {
			go populateUsers(users)
		} else {
			panic("Unable to get-bucket `users`")
		}
	}
	// Start pumping projects mutations if user-production file is available
	if options.projProd != "" {
		if projects, err := pool.GetBucket("projects"); err == nil {
			go populateProjects(projects)
		} else {
			panic("Unable to get-bucket `users`")
		}
	}

	//rpcconn, err := net.Dial("tcp", "localhost:8096")
	//if err != nil {
	//    panic(err)
	//}
	//defer rpcconn.Close()
	//c := jsonrpc.NewClient(rpcconn)

	eventch := make(chan *goupr.StreamEvent)
	var streams Streamer
	switch options.proto {
	case "tap":
		streams = NewTapStreams(pool, nil, eventch)
	case "upr":
		streams = NewUprStreams(pool, eventch)
	}

	go streams.OpenStreams()
	for {
		serverUuid, indexinfos, err := imanager.List("")
		indexinfos = []api.IndexInfo{api.IndexInfo{
			Bucket: "gamesim-sample",
		}}
		if err != nil {
			panic(err)
		}
		streams.UpdateIndexInfos(indexinfos)
		bucketexprs := parseExpression(indexinfos)
		notifych := make(chan string, 1)
		go waitNotify(imanager, serverUuid, notifych)
		loop(nil, notifych, eventch, bucketexprs)
		log.Println("Projector restarting ...")
	}
	streams.CloseStreams()
}

func loop(c *rpc.Client, notifych chan string, eventch chan *goupr.StreamEvent,
	bucketexprs map[*api.IndexInfo][]ast.Expression) {

	count := 0
Loop:
	for {
		select {
		case serverUuid := <-notifych:
			log.Println("Notification received, serverUuid:", serverUuid)
			break Loop
		case sevent := <-eventch:
			log.Println("StreamEvent", sevent.Bucket, count)
			count += 1
			for indexinfo, astexprs := range bucketexprs {
				bucket, idxuuid := indexinfo.Bucket, indexinfo.Uuid
				if sevent.Bucket != bucket {
					continue
				}
				m := api.Mutation{
					Type:    sevent.Opstr,
					Indexid: idxuuid,
					Docid:   string(sevent.Key),
				}
				if indexinfo.IsPrimary {
					m.Value = sevent.Value
				} else {
					m.SecondaryKey = evaluate(sevent.Value, astexprs)
				}
				log.Println("mutation recevied", sevent.Opstr, idxuuid, bucket, m.Docid,
					m.SecondaryKey)
				//var r bool
				//err := c.Call("MutationManager.ProcessSingleMutation", m, &r)
				//if err != nil {
				//    log.Fatal("Mutation error:", err)
				//}
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
	for _, indexinfo := range indexinfos {
		astexprs := make([]ast.Expression, 0)
		for _, expr := range indexinfo.OnExprList {
			if ex, err := ast.UnmarshalExpression([]byte(expr)); err == nil {
				astexprs = append(astexprs, ex)
			} else {
				panic(err)
			}
		}
		bucketexprs[&indexinfo] = astexprs
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
