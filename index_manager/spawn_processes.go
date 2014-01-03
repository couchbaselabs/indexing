// Notes: the program does not care the JSON doc
// is from who and where.
// This program will be expanded to:
// (1) include more JSON specs/formats.
// (2) one JSON doc has multiple JSON structures
// (3) watchdog timer for spawned processes
// (4) recovery in case of process crashes
// (5) deal with repeated failures of startup
//     due to resoure constraints or other reasons
//     like network connection failures, etc.
// (6) a REST API support to stop/suspend/kill/cleanup
//     the spawned processes manually by the admin
// (7) more stuff to be added
//
// For now, the workflow is simply as follows:
// (a) read the JSON doc into jsonBlob []byte
// (b)  define the corresponding type struct
// (c) parse the JSON with json.unmarshal
// (d)  spawn processes

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
)

//check errors
func check(e error) {
	if e != nil {
		fmt.Println("error:", e)
		panic(e)
	}
}

func main() {

	type Process struct {
		Name    string
		Path    string
		Cmdargs []string
		Env     []string
	}

    type Processes struct {
        Processes   []Process
    }
    //read the JSON doc
	jsonBlob, err := ioutil.ReadFile("JSONdoc.txt")
	check(err)
	fmt.Print(string(jsonBlob))

    //JSON parser
	var processes Processes
	err = json.Unmarshal(jsonBlob, &processes)
	check(err)
	fmt.Printf("%+v", processes)

    //spawn processes
    for i := 0; i < len(processes.Processes)-1; i++ { 
	    cmd := exec.Command(processes.Processes[i].Name)
	    cmdOut, err := cmd.Output()
	    check(err)
	    fmt.Println(string(cmdOut))
    }
}
