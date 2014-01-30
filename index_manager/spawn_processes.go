// Notes: the program does not care the JSON doc
// is from who and where.
// This program will be expanded to:
// (1) include more JSON specs/formats.
// (2) one JSON doc has multiple JSON structures
// (3) Log output to separate file per process using Go’s logging framework
// (4) watchdog timer for spawned processes
// (5) recovery in case of process crashes
// (6) deal with repeated failures of startup
//     due to resoure constraints or other reasons
//     like network connection failures, etc.
// (7) a REST API support to stop/suspend/kill/cleanup
//     the spawned processes manually by the admin
// (8) more stuff to be added
//
// For now, the workflow is simply as follows:
// (a) read the JSON doc
// (b) parse the JSON
// (c) spawn processes
// (d) logging the output
// (e) starts watchdog timer if needed for each process
// (f) restart the process if it exits with error
// (g) ...

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"
)

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

//if there is no reset, it is in idle state
//if there is reset, decrement the timer
//till it times out and set the timeout
//channel value to true
func (wd *Watchdog) loop() {
	var t0 int64
idle:
	t0 = <-wd.resets
	t0 = wd.pump(t0)
loop:
	t0 = t0 - time.Now().UnixNano()
	time.Sleep(time.Second * time.Duration(t0))
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

//this is to set the timeout in Seconds
//use absolute time value to avoid miscalculations
func (wd *Watchdog) reset(timeoutSecs int64) {
	wd.resets <- timeoutSecs + time.Now().UnixNano()
}

//check timeouts, note that we may need to add additional
//channel to coordinate
func check_timeouts(wd *Watchdog,
	logFile *os.File, pre int) {
	tmo := <-wd.timeouts
	if tmo == true {
		fmt.Fprintf(logFile, "Timed out!")
		// TO ADD: we may need to terminate
		// the spawned process forcefully here
		// if it is still running
	}
	//no need to continue the timer if process already ends
	if pre == 1 {
		wd.reset(0)
	}
}

//run procs, if it fails, we need to retry
//with configuratable delay time and maximum
//number of tries
func run_procs(cmd *exec.Cmd, wd *Watchdog,
	logFile *os.File, delaySecs int64,
	numRetries int, tmo int64) {
	//check the timeout with a goroutine
	go check_timeouts(wd, logFile, 0)
restart:
	//run the process and returns the output
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
		tmo = 5
		delaySecs = 1
		numRetries = 3
		wdg[i].reset(tmo)

        //spawn and run the processes
		cmd = exec.Command(processes.Processes[i].Name, processes.Processes[i].Cmdargs)
		run_procs(cmd, wdg[i], logFile, delaySecs, numRetries, tmo)
	}
}
