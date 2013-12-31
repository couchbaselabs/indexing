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
    memcached "github.com/dustin/gomemcached"
    mc "github.com/dustin/gomemcached/client"
    "github.com/prataprc/golib"
    "github.com/prataprc/monster"
    "log"
    "math/rand"
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
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
    flag.IntVar(&options.port, "port", 11211, "Host to connect")
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

    rpcconn, err := net.Dial("tcp", "localhost:8096")
    if err != nil {
        panic(err)
    }
    defer rpcconn.Close()
    c := jsonrpc.NewClient(rpcconn)

    for {
        serverUuid, indexinfos, err := imanager.List("")
        fmt.Println(indexinfos)
        if err != nil {
            panic(err)
        }
        notifych := make(chan string, 1)
        tapch := make(chan []interface{}, 100)
        bucketexprs := parseExpression(indexinfos)
        feeds := tapStream(pool, indexinfos, tapch)
        go waitNotify(imanager, serverUuid, notifych)
        loop(c, notifych, tapch, bucketexprs)
        closeFeeds(feeds)
    }
}

var tapop2type = map[memcached.CommandCode]string{
    memcached.TAP_MUTATION: "INSERT",
    memcached.TAP_DELETE:   "DELETE",
}

func loop(c *rpc.Client, notifych chan string, tapch chan []interface{},
    bucketexprs map[[3]string][]ast.Expression) {

    for {
        select {
        case <-notifych:
            break
        case msg := <-tapch:
            uuid, bucket := msg[0].(string), msg[1].(string)
            tevent := msg[2].(mc.TapEvent)
            for idx, astexprs := range bucketexprs {
                if idx[0] != bucket {
                    continue
                }
                m := api.Mutation{
                    Type:         tapop2type[memcached.CommandCode(tevent.Opcode)],
                    Indexid:      uuid,
                    SecondaryKey: evaluate(tevent.Value, astexprs),
                    Docid:        string(tevent.Key),
                }
                log.Println(m)
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

func waitNotify(imanager *imclient.RestClient, serverUuid string, ch chan string) {
    if _, serverUuid, err := imanager.Notify(serverUuid); err != nil {
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
        args := mc.TapArguments{
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
        go runFeed(feed, indexinfo, tapch)
    }
    return
}

func runFeed(feed *couchbase.TapFeed, indexinfo api.IndexInfo, tapch chan []interface{}) {
    bucket := indexinfo.Bucket
    log.Println("feed for bucket", bucket, "...")
    for {
        if event, ok := <-feed.C; ok {
            tapch <- []interface{}{indexinfo.Uuid, bucket, event}
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
