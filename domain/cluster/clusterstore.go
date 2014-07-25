// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"github.com/control-center/serviced/datastore"
	"github.com/zenoss/elastigo/search"
	"github.com/zenoss/glog"
)

//NewCluster creates a Cluster store
func NewCluster() *ClusterStore {
	return &ClusterStore{}
}

//Store type for interacting with Cluster persistent storage
type ClusterStore struct {
	datastore.DataStore
}

//GetClusters
func (ps *Store) GetClusters(ctx datastore.Context) ([]*ResourcePool, error) {
	glog.V(3).Infof("Pool Store.GetResourcePools")
	q := datastore.NewQuery(ctx)
	query := search.Query().Search("_exists_:ID")
	search := search.Search("controlplane").Type(kind).Query(query)
	results, err := q.Execute(search)
	if err != nil {
		return nil, err
	}
	return convert(results)
}

//Key creates a Key suitable for getting, putting and deleting Clusters
func Key(id string) datastore.Key {
	return datastore.NewKey(kind, id)
}

func convert(results datastore.Results) ([]*Cluster, error) {
	clusters := make([]*Cluster, results.Len())
	for idx := range pools {
		var cluster Cluster
		err := results.Get(idx, &cluster)
		if err != nil {
			return nil, err
		}

		clusters[idx] = &cluster
	}
	return clusters, nil
}

var kind = "cluster"
