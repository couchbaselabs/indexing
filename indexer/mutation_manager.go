package main

import (
	"errors"
	"github.com/couchbaselabs/indexing/api"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type MutationManager struct {
	indexmap map[string]api.Finder
}

var mutationMgr MutationManager

type ddlNotification struct {
	indexinfo api.IndexInfo
	ddltype   api.RequestType
}

func (m *MutationManager) ProcessSingleMutation(mutation *api.Mutation, reply *bool) error {
	log.Printf("Received Mutation Type %s Indexid %v, Docid %v, Vbucket %v, Seqno %v", mutation.Type, mutation.Indexid, mutation.Docid, mutation.Vbucket, mutation.Seqno)

	//FIXME change this to channel based
	*reply = false

	if mutation.Type == "INSERT" {

		var key api.Key
		var value api.Value
		var err error

		if key, err = api.NewKey(mutation.SecondaryKey, mutation.Docid); err != nil {
			log.Printf("Error Generating Key From Mutation %v", err)
			*reply = false
			return err
		}

		if value, err = api.NewValue(mutation.SecondaryKey, mutation.Docid, mutation.Vbucket, mutation.Seqno); err != nil {
			log.Printf("Error Generating Value From Mutation %v", err)
			*reply = false
			return err
		}

		if engine, ok := m.indexmap[mutation.Indexid]; ok {
			if err := engine.InsertMutation(key, value); err != nil {
				log.Printf("Error from Engine during InsertMutation %v", err)
				*reply = false
				return err
			}
		} else {
			log.Printf("Unknown Index %v or Engine not found", mutation.Indexid)
			*reply = false
			return errors.New("Unknown Index or Engine not found")
		}

		*reply = true

	} else if mutation.Type == "DELETE" {

		if engine, ok := m.indexmap[mutation.Indexid]; ok {
			if err := engine.DeleteMutation(mutation.Docid); err != nil {
				log.Printf("Error from Engine during Delete Mutation %v", err)
				*reply = false
				return err
			}
		} else {
			log.Printf("Unknown Index %v or Engine not found", mutation.Indexid)
			*reply = false
			return errors.New("Unknown Index or Engine not found")
		}
		*reply = true

	}
	return nil
}

func StartMutationManager() (chan ddlNotification, error) {

	var err error
	//start the rpc server
	if err = startRPCServer(); err != nil {
		return nil, err
	}

	//create a channel to receive notification from indexer
	//and start a goroutine to listen to it
	chnotify = make(chan ddlNotification)
	go acceptIndexerNotification(chnotify)
	return chnotify, nil
}

func startRPCServer() error {

	log.Println("Starting Mutation Manager")
	server := rpc.NewServer()
	server.Register(&mutationMgr)

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	mutationMgr.indexmap = make(map[string]api.Finder)

	l, err := net.Listen("tcp", ":8096")
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error in Accept %v. Shutting down")
				//FIXME Add a cleanup function
				return
			}
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}()
	return nil

}

func acceptIndexerNotification(chnotify chan ddlNotification) {

	ok := true
	var ddl ddlNotification
	for ok {
		ddl, ok = <-chnotify
		if ok {
			switch ddl.ddltype {
			case api.CREATE:
				mutationMgr.indexmap[ddl.indexinfo.Uuid] = ddl.indexinfo.Engine
			case api.DROP:
				delete(mutationMgr.indexmap, ddl.indexinfo.Uuid)
			default:
				log.Printf("Mutation Manager Received Unsupported Notification %v", ddl.ddltype)
			}
		}
	}
}
