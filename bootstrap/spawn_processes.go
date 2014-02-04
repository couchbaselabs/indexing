/* Notes: the program does not care the JSON doc
is from who and where.
This program will be expanded to:
(1) include more JSON specs/formats.
(2) one JSON doc has multiple JSON structures
(3) Log output to separate file per process using Go’s logging framework
(4) watchdog timer for spawned processes
(5) recovery in case of process crashes
(6) deal with repeated failures of startup
    due to resoure constraints or other reasons
    like network connection failures, etc.
(7) a REST API support to stop/suspend/kill/cleanup
    the spawned processes manually by the admin
(8) more stuff to be added

For now, the workflow is simply as follows:
(a) read the JSON doc
(b) parse the JSON
(c) spawn processes
(d) logging the output
(e) starts watchdog timer if needed for each process
(f) restart the process if it exits with error
(g) ...
*/

/* REST API operations:

GET = Retrieve a representation of a resource, i.e., the status of a process:
    process ID, running time, etcs. Those will be encoded in JSON format
POST = Create if you are sending content to the server to create a
subordinate of the specified resource collection, using some server-side algorithm.
PUT = Create iff you are sending the full content of the specified resource (URI),
    the operations for a process: suspend/kill/stop/cleanup a given process
PUT = Update iff you are updating the full content of the specified resource.
DELETE = Delete if you are requesting the server to delete the resource,
    to delete the info/resource that is related to a given process
PATCH = Update partial content of a resource
OPTIONS = Get information about the communication options for the request URI

Both Request and Response can Unmarshal and Marshal objects to and from JSON
using the standard packages

*/

/*
const (

        StatusOK                   = 200
        StatusCreated              = 201
        StatusAccepted             = 202
        StatusNonAuthoritativeInfo = 203
        StatusNoContent            = 204
        StatusResetContent         = 205
        StatusPartialContent       = 206

        StatusUseProxy          = 305
        StatusTemporaryRedirect = 307

        StatusBadRequest                   = 400
        StatusUnauthorized                 = 401
        StatusNotFound                     = 404
        StatusRequestTimeout               = 408
        StatusConflict                     = 409
        StatusGone                         = 410
        StatusLengthRequired               = 411
        StatusPreconditionFailed           = 412
        StatusRequestEntityTooLarge        = 413
        StatusRequestURITooLong            = 414
        StatusUnsupportedMediaType         = 415
        StatusRequestedRangeNotSatisfiable = 416
        StatusExpectationFailed            = 417
        StatusTeapot                       = 418

        StatusInternalServerError     = 500
        StatusNotImplemented          = 501
        StatusGatewayTimeout          = 504
)
*/

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"
)

const (
	GET     = "GET"
	POST    = "POST"
	PUT     = "PUT"
	DELETE  = "DELETE"
	PATCH   = "PATCH"
	OPTIONS = "OPTIONS"
)

const (
	Application_Json = "application/json"
)

//config server type definition
type ConfigServer struct {
	mux *http.ServeMux
}

//allocates and returns a new config server
func newConfigServer() *ConfigServer {
	return &ConfigServer{}
}

type Resourcer interface {
	Get(values url.Values) (int, interface{})
	Post(values url.Values) (int, interface{})
	Put(values url.Values) (int, interface{})
	Delete(values url.Values) (int, interface{})
	Patch(values url.Values) (int, interface{})
	Options(values url.Values) (int, interface{})
}

type Watchdog struct {
	resets   chan int64
	timeouts chan bool
}

//create a new watchdog
func newWatchdog() *Watchdog {
	wd := &Watchdog{
		resets: make(chan int64, 100),

		timeouts: make(chan bool, 2),
	}
	//init. as false to coordinate events
	wd.timeouts <- false
	go wd.loop()
	return wd
}

//check if there are new resets
func (wd *Watchdog) pump(t0 int64) (t1 int64) {
	t1 = t0
	for {
		select {
		case t := <-wd.resets:
			if t > t0 {
				t1 = t
			}
		default:
			return
		}
	}
	panic("unreachable")
}

/*  if there is no reset, it is in idle state
    if there is reset, decrement the timer
    till it times out and set the timeout
    channel value to true
*/
func (wd *Watchdog) loop() {
	var t0 int64
idle:
	t0 = <-wd.resets
	t0 = wd.pump(t0)
loop:
	t0 = t0 - time.Now().UnixNano()
	time.Sleep(time.Duration(t0))
	now := time.Now().UnixNano()
	t0 = wd.pump(now)
	if t0 == now {
		wd.timeouts <- true
		goto idle
	}
	goto loop
}

//check errors
func check(e error) {
	if e != nil {
		fmt.Println("error:", e)
		panic(e)
	}
}

//this is to set the timeout in NanoSeconds
//use absolute time value to avoid miscalculations
func (wd *Watchdog) reset(timeoutNanoSecs int64) {
	wd.resets <- timeoutNanoSecs + time.Now().UnixNano()
}

//check timeouts, note that we may need to add additional
//channel to coordinate
func check_timeouts(wd *Watchdog,
	logFile *os.File, pre int) {
	tmo := <-wd.timeouts
	if tmo == true {
		fmt.Fprintf(logFile, "Timed out!")
		/* TO ADD: we may need to terminate
		   the spawned process forcefully here
		   if it is still running
		*/
	}
	/* no need to continue the timer if process already ends
	   TO CHANGE: later on to use additional channel for
	   the coordination between timeout_checking and
	   process completion
	*/
	if pre == 1 {
		wd.reset(0)
	}
}

/*  run procs, if it fails, we need to retry
    with configuratable delay time and maximum
    number of tries
*/
func run_procs(cmd *exec.Cmd, wd *Watchdog,
	logFile *os.File, delaySecs int64,
	numRetries int, tmo int64) {
	//check the timeout with a goroutine
	go check_timeouts(wd, logFile, 0)
restart:
	/* run the process and returns the output
	   TO CHANGE: just call cmd.Start() without waiting to
	   complete and use additional channel to coordinate
	   with timeout_checking
	*/
	cmdOut, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
		//restart if it errors out
		numRetries--
		if numRetries >= 0 {
			//delay for a while
			time.Sleep(time.Second * time.Duration(delaySecs))
			//reset the watchdog timer
			wd.reset(tmo)
			//retry
			goto restart
		}
	}
	go check_timeouts(wd, logFile, 1)

	//deal with the output
	log.Println(string(cmdOut))
	fmt.Fprintf(logFile, string(cmdOut))
	//wait until operation on logFile is done, close it
	defer logFile.Close()
}

//handling request for config server
func (cs *ConfigServer) requestHandler(rs Resourcer) http.HandlerFunc {
	return func(rw http.ResponseWriter, rq *http.Request) {

		if rq.ParseForm() != nil {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		var handler func(url.Values) (int, interface{})

		switch rq.Method {
		case GET:
			handler = rs.Get
		case POST:
			handler = rs.Post
		case PUT:
			handler = rs.Put
		case DELETE:
			handler = rs.Delete
		case PATCH:
			handler = rs.Patch
		case OPTIONS:
			handler = rs.Options
		}

		if handler == nil {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		code, data := handler(rq.Form)

		content, err := json.Marshal(data)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(code)
		rw.Write(content)
	}
}

//add a new resoure to a config server
// note that the "/" pattern matches everything
func (cs *ConfigServer) addResource(rs Resourcer, paths ...string) {
	if cs.mux == nil {
		cs.mux = http.NewServeMux()
	}
	for _, path := range paths {
		cs.mux.HandleFunc(path, cs.requestHandler(rs))
	}
}

//start the config web server
func (cfg *ConfigServer) Start(port int) {
	portString := fmt.Sprintf(":%d", port)
	http.ListenAndServe(portString, nil)
}

func main() {

	type Process struct {
		Name    string   `json:"name"`
		Path    string   `json:"path"`
		Cmdargs string   `json:"cmdargs"`
		Env     []string `json:"env"`
	}

	type Processes struct {
		Processes []Process `json:"processes"`
	}

	//read the JSON doc
	jsonBlob, err := ioutil.ReadFile("JSONdoc.txt")
	check(err)
	fmt.Print(string(jsonBlob))

	//JSON parser
	var processes Processes
	var logFile *os.File

	err = json.Unmarshal(jsonBlob, &processes)
	check(err)
	fmt.Printf("%+v", processes)
	fmt.Println()

	//watchdog for each process
	var tmo int64
	var delaySecs int64
	var numRetries int
	var wdg []*Watchdog
	wdg = make([]*Watchdog, len(processes.Processes))

	//spawning processes
	for i := 0; i <= len(processes.Processes)-1; i++ {
		fileName := "logfile"
		//log file name is based on date/time
		cmd := exec.Command("date")
		cmdOut, err := cmd.Output()
		check(err)
		fileName += string(cmdOut) + "_" + string(48+i)
		logFile, err = os.Create(fileName)
		check(err)

		//test only for the watchdogs
		wdg[i] = newWatchdog()
		//set the timeout as 5 Seconds in the test, it should change later
		//based on the application scenarios
		tmo = 5e9
		delaySecs = 1
		numRetries = 3
		wdg[i].reset(tmo)

		//spawn and run the processes
		cmd = exec.Command(processes.Processes[i].Name, processes.Processes[i].Cmdargs)
		run_procs(cmd, wdg[i], logFile, delaySecs, numRetries, tmo)
	}
}
