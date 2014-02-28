package cap

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"log"
	"testing"
)

var testCapmsg = Cap{
	Version:  proto.Uint32(10),
	Command:  proto.Int32(int32(Command_EndCommand)),
	Request:  proto.Bool(true),
	Status:   proto.Int32(int32(Status_EndStatus)),
	Opaque:   proto.Uint32(10000),
	Masterid: proto.Uint64(0xFFFFFFFFFFFFFFFF),
}

var testPoolmsg = Pool{
	Nodes: []string{(*proto.String("I am an 40 byte string ................."))},
}

var testIndexinfomsg = IndexInfo{
	Id:         proto.Uint32(1000000),
	Name:       proto.String("simple-index"),
	Using:      proto.Int32(int32(IndexType_EndIndexType)),
	Bucket:     proto.String("default"),
	IsPrimary:  proto.Bool(false),
	ExprType:   proto.Int32(int32(ExprType_EndExprType)),
	OnExprList: []string{(*proto.String("dummy expression string"))},
}

var testMutation = Mutation{
	Docid:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
	Vbucket:  proto.Int32(1024),
	Vbuuid:   proto.Uint64(0xFFFFFFFFFFFFFFFF),
	Sequence: proto.Uint64(0xFFFFFFFFFFFFFFFF),
	Keys: [][]byte{
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0}},
	Indexid: []uint32{(*proto.Uint32(4000000000))},
}

func TestCap(t *testing.T) {
	data, err := proto.Marshal(&testMutation)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	fmt.Println("Encoded mutation length", len(data))

	data, err = proto.Marshal(&testCapmsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	newmsg := Cap{}
	err = proto.Unmarshal(data, &newmsg)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	// check data
	if x, y := testCapmsg.GetVersion(), newmsg.GetVersion(); x != y {
		log.Fatalf("version mismatch %q != %q", x, y)
	}
	if x, y := testCapmsg.GetCommand(), newmsg.GetCommand(); x != y {
		log.Fatalf("Command mismatch %q != %q", x, y)
	}
	if x, y := testCapmsg.GetRequest(), newmsg.GetRequest(); x != y {
		log.Fatalf("Request mismatch %q != %q", x, y)
	}
	if x, y := testCapmsg.GetStatus(), newmsg.GetStatus(); x != y {
		log.Fatalf("Status mismatch %q != %q", x, y)
	}
	if x, y := testCapmsg.GetOpaque(), newmsg.GetOpaque(); x != y {
		log.Fatalf("Opaque mismatch %q != %q", x, y)
	}
	if x, y := testCapmsg.GetMasterid(), newmsg.GetMasterid(); x != y {
		log.Fatalf("Masterid mismatch %q != %q", x, y)
	}
}

func BenchmarkCapMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		proto.Marshal(&testCapmsg)
	}
}

func BenchmarkCapUnmarshal(b *testing.B) {
	data, _ := proto.Marshal(&testCapmsg)
	newmsg := Cap{}
	for i := 0; i < b.N; i++ {
		proto.Unmarshal(data, &newmsg)
	}
}

func BenchmarkPoolMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		proto.Marshal(&testPoolmsg)
	}
}

func BenchmarkPoolUnmarshal(b *testing.B) {
	data, _ := proto.Marshal(&testPoolmsg)
	newmsg := Cap{}
	for i := 0; i < b.N; i++ {
		proto.Unmarshal(data, &newmsg)
	}
}

func BenchmarkIndexInfoMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		proto.Marshal(&testIndexinfomsg)
	}
}

func BenchmarkIndexInfoUnmarshal(b *testing.B) {
	data, _ := proto.Marshal(&testIndexinfomsg)
	newmsg := Cap{}
	for i := 0; i < b.N; i++ {
		proto.Unmarshal(data, &newmsg)
	}
}

func BenchmarkMutationMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		proto.Marshal(&testMutation)
	}
}

func BenchmarkMutationUnmarshal(b *testing.B) {
	data, _ := proto.Marshal(&testMutation)
	newmsg := Cap{}
	for i := 0; i < b.N; i++ {
		proto.Unmarshal(data, &newmsg)
	}
}
