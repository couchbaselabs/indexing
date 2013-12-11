package main

import (
	"errors"
	"github.com/couchbaselabs/indexing/api"
	"reflect"
	"testing"
	"time"
)

//FIXME Change from t.Error to t.Errorf
func TestRestClient(t *testing.T) {
	client := NewRestClient("http://localhost:8094")
	indexinfo := api.IndexInfo{
		Name:       "test",
		Using:      api.Llrb,
		OnExprList: []string{`{"type":"property","path":"age"}`},
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
	}
	servUuid := ""

	// Create()
	if servUuid_, indexinfo_, err := client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	} else {
		servUuid, indexinfo = servUuid_, indexinfo_
	}

	// Create Duplicate
	if _, _, err := client.Create(indexinfo); err == nil {
		t.Error("Duplicate Index Created", indexinfo.Name)
	} 

	// List("")
	if servUuid_, indexinfos, err := client.List(""); err != nil {
		t.Error("List() call failed", err)
	} else if servUuid_ != servUuid {
		t.Error("Server uuid does not match", servUuid, servUuid_)
	} else {
		if len(indexinfos) != 1 {
			t.Error("Finding more than one index on the server")
		} else if indexinfos[0].Name != indexinfo.Name {
			t.Errorf("Unable to find index %v on server", indexinfo.Name)
		} else if indexinfos[0].Uuid != indexinfo.Uuid {
			t.Errorf("Unable to find index %v on server", indexinfo.Name)
		}
	}

	// List(servUuid)
	if servUuid_, indexinfos, err := client.List(servUuid); err != nil {
		t.Error("List() call failed", err)
	} else if servUuid_ != servUuid {
		t.Error("Server uuid does not match")
	} else if len(indexinfos) != 0 {
		t.Error("Expecting an empty list")
	}

	// Index()
	if indexinfo_, err := client.Index(indexinfo.Uuid); err != nil {
		t.Error("Index() call failed", err)
	} else if !reflect.DeepEqual(indexinfo_.OnExprList, indexinfo.OnExprList) {
		t.Error("OnExprList returned by Index() call does not match")
	}

	// Nodes()
	if nodes, err := client.Nodes(); err != nil {
		t.Error("Nodes() failed", err)
	} else if node := nodes[0]; node.IndexerURL != "http://localhost:8095" {
		t.Error("nodes does not match", nodes)
	}

	// Drop()
	if servUuid_, err := client.Drop(indexinfo.Uuid); err != nil {
		t.Error("Unable to drop index", indexinfo.Name, err)
	} else if servUuid == servUuid_ {
		t.Error("Expected a different server unique id after a drop")
	} else {
		servUuid = servUuid_
	}

	// List("")
	if servUuid_, indexinfos, err := client.List(""); err != nil {
		t.Error("List() call failed", err)
	} else if servUuid_ != servUuid {
		t.Error("Server uuid does not match")
	} else if len(indexinfos) != 0 {
		t.Error("Expecting zero index on the server")
	}
}

func TestNotify(t *testing.T) {
	var err error

	client := NewRestClient("http://localhost:8094")
	indexinfo := api.IndexInfo{
		Name:       "test",
		Using:      api.Llrb,
		OnExprList: []string{`{"type":"property","path":"age"}`},
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
	}
	servUuid := ""

	if servUuid, indexinfo, err = client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	}
	if servUuid, err = client.Drop(indexinfo.Uuid); err != nil {
		t.Error("Unable to drop index", indexinfo.Name, err)
	}

	// Notify()
	respch := startNotificationReceiver(t, client, servUuid)
	time.Sleep(time.Millisecond * 1000)

	// Create()
	if servUuid, indexinfo, err = client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	}

	if notifyServerUuid, errNotify := waitNotifyResponseOrTimeout(respch); errNotify != nil {
		t.Error(errNotify)
	} else if notifyServerUuid != servUuid {
		t.Error("Notify returned serverUuid doesn't match expected Uuid")
	}

	respch = startNotificationReceiver(t, client, servUuid)
	if _, err = waitNotifyResponseOrTimeout(respch); err == nil {
		t.Error("Expecting timeout err")
	}

}

func handleNotification(t *testing.T, client *RestClient, cacheServUuid string,
	respch chan string) {

	status, serverUuid, err := client.Notify(cacheServUuid)
	if err != nil {
		t.Error("Error in notification", err)
	}
	if status != api.INVALID_CACHE {
		t.Error("Invalid notification message", status)
	}
	if serverUuid == cacheServUuid {
		t.Error("Received Same ServerUUID in notification as sent in request. Invalid.")
	}
	respch <- serverUuid
}

func startNotificationReceiver(t *testing.T, client *RestClient,
	cacheServUuid string) chan string {

	respch := make(chan string, 1)
	go handleNotification(t, client, cacheServUuid, respch)
	return respch

}

func waitNotifyResponseOrTimeout(respch chan string) (string, error) {
	timeoutch := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeoutch <- true
	}()

	select {
	case serverUuid := <-respch:
		return serverUuid, nil
	case <-timeoutch:
		return "", errors.New("Timeout: did not receive notification")
	}
}
