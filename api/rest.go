// REST API to access indexing.

// TODO: Add STATS command.

package api

type RequestType int

const (
    CREATE RequestType = iota   // /create
    DROP                        // /drop
    LIST                        // /list
    SCAN                        // /scan
    STATS                       // /stats
    NODES                       // /nodes
    NOTIFY                      // /notify
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
    Status  ResponseStatus
    Indexes []IndexInfo
    ServerUuid    string
    Nodes   []string
    Errors  []IndexError
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
