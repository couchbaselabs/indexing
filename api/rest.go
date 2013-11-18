// REST APIs
// TODO: Add STATS command.

package api

type RequestType int
const (
    CREATE RequestType = iota   // /create
    DROP                        // /drop
    LIST                        // /list
    SCAN                        // /scan
    STATS                       // /stats
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
    Params     QueryParams
}

//RESPONSE DATA FORMATS
type ResponseStatus int
const (
    SUCCESS ResponseStatus = iota
    ERROR
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
    Errors  []IndexError
}

type IndexScanResponse struct {
    Status    ResponseStatus
    TotalRows int64
    Rows      []IndexRow
    Errors    []IndexError
}
