package main

import (
	"github.com/couchbaselabs/indexing/api"
	//    "github.com/couchbaselabs/dparval"
	"errors"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type Mutation struct {
	Type         string
	Indexid      string
	SecondaryKey [][]byte
	Docid        string
	Vbucket      int
	Seqno        int64
}

type MutationManager struct {
	indexmap map[string]api.Finder
}

var mutationMgr MutationManager

func (m *MutationManager) ProcessSingleMutation(mutation *Mutation, reply *bool) error {
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

func RegisterIndexWithMutationHandler(indexinfo api.IndexInfo) {
	mutationMgr.indexmap[indexinfo.Uuid] = indexinfo.Engine
}

func StartMutationManager() error {

	log.Println("Starting Mutation Manager")
	server := rpc.NewServer()
	server.Register(&mutationMgr)

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	mutationMgr.indexmap = make(map[string]api.Finder)

	l, e := net.Listen("tcp", ":8222")
	if e != nil {
		return e
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
	return nil
}
