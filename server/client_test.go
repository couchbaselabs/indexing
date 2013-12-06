package server

import (
	"errors"
	"github.com/couchbaselabs/indexing/api"
	"testing"
	"time"
)

func TestRestClient(t *testing.T) {
	client := NewRestClient("http://localhost:8094")
	indexinfo := api.IndexInfo{
		Name:       "test",
		Using:      api.Llrb,
		CreateStmt: `CREATE INDEX emailidx ON users (age+10)`,
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
		Expression: "",
	}
	servUuid := ""

	// Create()
	if servUuid_, indexinfo_, err := client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	} else {
		servUuid, indexinfo = servUuid_, indexinfo_
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
	} else if indexinfo_.CreateStmt != indexinfo.CreateStmt {
		t.Error("create statement returned by Index() call does not match")
	}

	// Nodes()
	if nodes, err := client.Nodes(); err != nil {
		t.Error("Nodes() failed", err)
	} else if nodes[0] != "localhost:8094" {
		t.Error("nodes does not match", nodes)
	}

	// Drop()
	if servUuid_, err := client.Drop(indexinfo.Uuid); err != nil {
		t.Error("Unable to drop index", indexinfo.Name, err)
	} else if servUuid == servUuid_ {
		t.Error("Expected a different server unique id after a dropt")
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
		CreateStmt: `CREATE INDEX emailidx ON users (age+10)`,
		Bucket:     "users",
		IsPrimary:  false,
		Exprtype:   "simple",
		Expression: "",
	}
	servUuid := ""

	if servUuid, indexinfo, err = client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	}
	if servUuid, err = client.Drop(indexinfo.Uuid); err != nil {
		t.Error("Unable to drop index", indexinfo.Name, err)
	}

	// Notify()
	errch := client.Notify(servUuid)

	// Create()
	if servUuid, indexinfo, err = client.Create(indexinfo); err != nil {
		t.Error("Unable to create index", indexinfo.Name, err)
	}
	if err = waitNotification(errch); err != nil {
		t.Error(err)
	}

	errch = client.Notify(servUuid)
	if err = waitNotification(errch); err == nil {
		t.Error("Expecting timeout err")
	}
}

func waitNotification(errch chan error) (err error) {
	// Wait for notification
	timeoutch := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeoutch <- true
	}()

	select {
	case err = <-errch:
		return err
	case <-timeoutch:
		return errors.New("Timeout: did not receive notification")
	}
}
