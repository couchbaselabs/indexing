package main

import (
	"log"
	"os/exec"
)

//number of parallel workers for each query
const numWorkerInstances = 5

//number of times each worker executes the query. 0 is infinite.
const numIterations = 0

var wait chan struct{}

func main() {

	//list of queries to execute
	queryList := []string{
		"select key_num from default where key_num > 5000 and key_num < 8000",
		"select key_num from default where key_num > 1000 and key_num < 50000",
		"select key_num from default where key_num > 1000000 and key_num < 1050000",
		"select key_num from default where key_num > 1000 and key_num < 500000",
		"select key_num from default where key_num > 2000000 and key_num < 2500000",
	}

	//iterate through the query list and run
	for i, q := range queryList {
		go runOneQuery(i, q)
	}

	//infinite wait
	<-wait
}

func runOneQuery(query_id int, query string) {

	for w := 0; w < numWorkerInstances; w++ {
		go startWorker(w, query_id, query)
	}
	//infinite wait
	<-wait

}

func startWorker(workerId, queryId int, query string) {

	log.Printf("Worker Started %v for QueryID %v", workerId, queryId)

	var loopCnt int = 0
	for {
		//Applications/log.Printf("Executing Query ID %v. Worker %v. Iteration %v", queryId, workerId, loopCnt)
		out, err := exec.Command("curl", "-X", "POST", "-H", "Content-Type:text/plain", "-d", query, "http://localhost:8093/query").Output()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Output Len %v. Query ID %v. Worker %v. Iteration %v", len(out), queryId, workerId, loopCnt)
		loopCnt += 1
		if loopCnt == numIterations {
			break
		}
	}

	log.Printf("Worker Stopped %v for QueryID %v", workerId, queryId)
}
