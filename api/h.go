//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at,
//          http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func TryConnection(initial, maximum time.Duration, fn func() bool) {
	retryInterval := initial
	for {
		if fn() == false {
			break
		} else {
			log.Printf("Retrying after %v seconds ...\n", retryInterval)
			<-time.After(retryInterval)
			if retryInterval *= 2; retryInterval > maximum {
				retryInterval = maximum * time.Second
			}
		}
	}
}

// Parse HTTP Request to get IndexInfo.
func RequestPayload(r *http.Request, payload *interface{}) (err error) {
	buf := make([]byte, r.ContentLength, r.ContentLength)
	r.Body.Read(buf)
	err = json.Unmarshal(buf, payload)
	return
}
