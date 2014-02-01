// REST API to access indexing.

// TODO: Implement STATS command.
// TODO: Change the server implementation URL to follow REST philosphy.

package api

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	LIST   RequestType = "list"
	NOTIFY RequestType = "notify"
	NODES  RequestType = "nodes"
	SCAN   RequestType = "scan"
	STATS  RequestType = "stats"
)

// All API accept IndexRequest structure and returns IndexResponse structure.
// If application is written in Go, and compiled with `indexing` package then
// they can choose the access the underlying interfaces directly.
type IndexRequest struct {
	Type       RequestType `json:"type,omitempty"`
	Index      IndexInfo   `json:"index,omitempty"`
	ServerUuid string      `json:"serverUuid,omitempty"`
	Params     QueryParams `json:"params,omitempty"`
}

// URL encoded query params
type QueryParams struct {
	ScanType  ScanType  `json:"scanType,omitempty"`
	Low       [][]byte  `json:"low,omitempty"`
	High      [][]byte  `json:"high,omitempty"`
	Inclusion Inclusion `json:"inclusion,omitempty"`
	Limit     int64     `json:"limit,omitempty"`
}

type ScanType string

const (
	COUNT      ScanType = "count"
	EXISTS     ScanType = "exists"
	LOOKUP     ScanType = "lookup"
	RANGESCAN  ScanType = "rangeScan"
	FULLSCAN   ScanType = "fullScan"
	RANGECOUNT ScanType = "rangeCount"
)

//RESPONSE DATA FORMATS
type ResponseStatus string

const (
	SUCCESS       ResponseStatus = "success"
	ERROR         ResponseStatus = "error"
	INVALID_CACHE ResponseStatus = "invalid_cache"
)

type IndexRow struct {
	Key   [][]byte `json:"key,omitempty"`
	Value string   `json:"value,omitempty"`
}

type IndexError struct {
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

type IndexMetaResponse struct {
	Status     ResponseStatus `json:"status,omitempty"`
	Indexes    []IndexInfo    `json:"indexes,omitempty"`
	ServerUuid string         `json:"serverUuid,omitempty"`
	Nodes      []NodeInfo     `json:"nodes,omitempty"`
	Errors     []IndexError   `json:"errors,omitempty"`
}

type IndexScanResponse struct {
	Status    ResponseStatus `json:"status,omitempty"`
	TotalRows uint64         `json:"totalrows,omitempty"`
	Rows      []IndexRow     `json:"rows,omitempty"`
	Errors    []IndexError   `json:"errors,omitempty"`
}

//Indexer Node Info
type NodeInfo struct {
	IndexerURL string `json:"indexerURL,omitempty"`
}
