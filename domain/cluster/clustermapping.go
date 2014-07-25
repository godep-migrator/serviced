// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/control-center/serviced/datastore/elastic"
	"github.com/zenoss/glog"
)

var (
	mappingString = `
{
    "resourcepool": {
      "properties":{
        "ID" :          {"type": "string", "index":"not_analyzed"},
        "Description" : {"type": "string", "index":"not_analyzed"},
      }
    }
}
`
	//MAPPING is the elastic mapping for a cluster
	MAPPING, mappingError = elastic.NewMapping(mappingString)
)

func init() {
	if mappingError != nil {
		glog.Fatalf("error creating cluster mapping: %v", mappingError)
	}
}
