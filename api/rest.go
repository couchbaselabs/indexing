// REST API to access indexing.

// TODO: Implement STATS command.
// TODO: Change the server implementation URL to follow REST philosphy.

package api

type RequestType int

const (
	CREATE RequestType = iota // POST /indexes/create
	DROP                      // DELETE /indexes/uuid
	LIST                      // GET /indexes/list
	SCAN                      // GET /indexes/uuid/scan
	STATS                     // GET /indexes/stats
	// GET /indexes/uuid/stats
	NODES  // GET /indexes/nodes
	NOTIFY // GET /indexes/notify
)

// URL encoded query params
type QueryParams struct {
	Low       Key
	High      Key
	Inclusion Inclusion
	Offset    int
	Limit     int
}

// All API accept IndexRequest structure and returns IndexResponse structure.
// If application is written in Go, and compiled with `indexing` package then
// they can choose the access the underlying interfaces directly.
type IndexRequest struct {
	Type       RequestType
	Indexinfo  IndexInfo
	ServerUuid string
	Params     QueryParams
}

//RESPONSE DATA FORMATS
type ResponseStatus int

const (
	SUCCESS ResponseStatus = iota
	ERROR
	INVALID_CACHE
)

type IndexRow struct {
	Key   string
	Value string
}

type IndexError struct {
	Code string
	Msg  string
}

type IndexMetaResponse struct {
	Status     ResponseStatus
	Indexes    []IndexInfo
	ServerUuid string
	Nodes      []string
	Errors     []IndexError
}

type IndexScanResponse struct {
	Status    ResponseStatus
	TotalRows int64
	Rows      []IndexRow
	Errors    []IndexError
}

//Indexer Node Info
type NodeInfo struct {
    IndexerURL string
}
