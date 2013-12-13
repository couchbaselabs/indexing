package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/server"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"github.com/dustin/gomemcached/client"
	"github.com/prataprc/golib"
	"github.com/prataprc/monster"
	"log"
	"math/rand"
	"strconv"
)

var options struct {
	host     string
	port     int
	userProd string
	projProd string
	seed     int
	count    int
}

func argParse() {
	flag.StringVar(&options.host, "host", "localhost", "Port to connect")
	flag.IntVar(&options.port, "port", 11212, "Host to connect")
	flag.StringVar(&options.userProd, "userProd", "", "monster production for users")
	flag.StringVar(&options.projProd, "projProd", "", "monster production for project")
	flag.IntVar(&options.seed, "seed", 100, "seed for production")
	flag.IntVar(&options.count, "count", 10, "number of documents to generate")
	flag.Parse()
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
	client := server.NewRestClient(imURL)
	// nodes := client.Nodes()

	notifych := make(chan string, 1)
	tapch := make(chan []interface{}, 100)

	if users, err := pool.GetBucket("users"); err == nil {
		go populateUsers(users)
	} else {
		panic("Unable to get-bucket `users`")
	}
	if projects, err := pool.GetBucket("projects"); err == nil {
		go populateProjects(projects)
	} else {
		panic("Unable to get-bucket `users`")
	}

	tryCreateIndex(client)
	for {
		serverUuid, indexinfos, err := client.List("")
		if err != nil {
			panic(err)
		}
		bucketexprs := parseExpression(indexinfos)

		feeds := tapStream(pool, indexinfos, tapch)
		go waitNotify(client, serverUuid, notifych)

		for {
			select {
			case <-notifych:
				closeFeeds(feeds)
				break
			case msg := <-tapch:
				bucket, tevent := msg[0].(string), msg[1].(memcached.TapEvent)
				for idx, astexprs := range bucketexprs {
					if idx[0] != bucket {
						continue
					}
					secKey := evaluate(tevent.Value, astexprs)
					log.Println(idx[0], idx[1], secKey, string(tevent.Key))
				}
			}
		}
	}
}

func evaluate(value []byte, astexprs []ast.Expression) []string {
	secKey := make([]string, 0)
	for _, expr := range astexprs {
		key, err := expr.Evaluate(dparval.NewValueFromBytes([]byte(value)))
		if err != nil {
			log.Println(err)
			secKey = append(secKey, "")
		} else {
			secKey = append(secKey, string(key.Bytes()))
		}
	}
	return secKey
}

func parseExpression(indexinfos []api.IndexInfo) map[[3]string][]ast.Expression {
	bucketexprs := make(map[[3]string][]ast.Expression)
	for _, indexinfo := range indexinfos {
		idx := [3]string{indexinfo.Bucket, indexinfo.Name, indexinfo.Uuid}
		astexprs := make([]ast.Expression, 0)
		for _, expr := range indexinfo.OnExprList {
			if ex, err := ast.UnmarshalExpression([]byte(expr)); err == nil {
				astexprs = append(astexprs, ex)
			} else {
				panic(err)
			}
		}
		bucketexprs[idx] = astexprs
	}
	return bucketexprs
}

func tryCreateIndex(client *server.RestClient) (err error) {
	if _, indexinfos, err := client.List(""); err != nil {
		panic(err)
	} else if len(indexinfos) == 4 {
		return nil
	}
	indexinfos := []api.IndexInfo{
		api.IndexInfo{
			Name:       "emailid",
			Using:      api.Llrb,
			OnExprList: []string{`{"type":"property","path":"emailid"}`},
			Bucket:     "users",
			IsPrimary:  false,
			Exprtype:   "simple",
		},
		api.IndexInfo{
			Name:       "age",
			Using:      api.Llrb,
			OnExprList: []string{`{"type":"property","path":"age"}`},
			Bucket:     "users",
			IsPrimary:  false,
			Exprtype:   "simple",
		},
		api.IndexInfo{
			Name:       "projects-members",
			Using:      api.Llrb,
			OnExprList: []string{`{"type":"property","path":"members"}`},
			Bucket:     "projects",
			IsPrimary:  false,
			Exprtype:   "simple",
		},
		api.IndexInfo{
			Name:       "project-language",
			Using:      api.Llrb,
			OnExprList: []string{`{"type":"property","path":"language"}`},
			Bucket:     "projects",
			IsPrimary:  false,
			Exprtype:   "simple",
		},
	}
	for _, indexinfo := range indexinfos {
		if _, _, err = client.Create(indexinfo); err != nil {
			panic(err)
		}
	}
	return
}

func waitNotify(client *server.RestClient, serverUuid string, ch chan string) {
	if _, serverUuid, err := client.Notify(serverUuid); err != nil {
		panic(err)
	} else {
		ch <- serverUuid
	}
}

func tapStream(pool couchbase.Pool, indexinfos []api.IndexInfo,
	tapch chan []interface{}) (feeds []*couchbase.TapFeed) {

	var bucket *couchbase.Bucket
	var err error

	for _, indexinfo := range indexinfos {
		if bucket, err = pool.GetBucket(indexinfo.Bucket); err != nil {
			panic(err)
		}
		args := memcached.TapArguments{
			Dump:       false,
			SupportAck: false,
			KeysOnly:   false,
			Checkpoint: true,
			ClientName: "indexer",
		}
		feed, err := bucket.StartTapFeed(&args)
		if err != nil {
			panic(err)
		}
		feeds = append(feeds, feed)
		go runFeed(feed, indexinfo.Bucket, tapch)
	}
	return
}

func runFeed(feed *couchbase.TapFeed, bucket string, tapch chan []interface{}) {
	log.Println("feed for bucket", bucket, "...")
	for {
		if event, ok := <-feed.C; ok {
			tapch <- []interface{}{bucket, event}
		} else {
			log.Println("closing tap feed for", bucket)
			break
		}
	}
}

func closeFeeds(feeds []*couchbase.TapFeed) {
	for _, feed := range feeds {
		feed.Close()
	}
}

func populateUsers(bucket *couchbase.Bucket) {
	// Map of interfaces can receive any value types
	value := map[string]interface{}{}

	if options.userProd == "" {
		return
	}

	conf := make(golib.Config)
	start := monster.Parse(options.userProd, conf)

	c := make(monster.Context)
	nonterminals, root := monster.Build(start)
	c["_random"] = rand.New(rand.NewSource(int64(options.seed)))
	c["_nonterminals"] = nonterminals
	for i := 0; i < options.count; i++ {
		jsbytes := []byte(root.Generate(c))
		if err := json.Unmarshal(jsbytes, &value); err != nil {
			panic(err)
		}
		docid := fmt.Sprintf("user%v", i)
		bucket.Set(docid, 0, value)
	}
}

func populateProjects(bucket *couchbase.Bucket) {
	// Map of interfaces can receive any value types
	value := map[string]interface{}{}

	if options.projProd == "" {
		return
	}

	conf := make(golib.Config)
	start := monster.Parse(options.projProd, conf)

	c := make(monster.Context)
	nonterminals, root := monster.Build(start)
	c["_random"] = rand.New(rand.NewSource(int64(options.seed)))
	c["_nonterminals"] = nonterminals
	for i := 0; i < options.count; i++ {
		jsbytes := []byte(root.Generate(c))
		if err := json.Unmarshal(jsbytes, &value); err != nil {
			panic(err)
		}
		docid := fmt.Sprintf("project%v", i)
		bucket.Set(docid, 0, value)
	}
}
