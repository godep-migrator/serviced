// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package api

import (
	"github.com/control-center/serviced/domain/cluster"
	"github.com/control-center/serviced/facade"
)

const ()

var ()

// ClusterConfig is the deserialized data from the command-line
type ClusterConfig struct {
	ClusterID string
}

// Returns a list of all pools
func (a *api) GetClusters() ([]*cluster.Cluster, error) {
	client, err := a.connectMaster()
	if err != nil {
		return nil, err
	}

	return client.GetCluster()
}

// Gets information about a cluster given a ClusterID
func (a *api) GetCluster(id string) (*cluster.Cluster, error) {
	client, err := a.connectMaster()
	if err != nil {
		return nil, err
	}

	return client.GetCluster(id)
}

// Adds a new cluster
func (a *api) AddCluster(config ClusterConfig) (*cluster.Cluster, error) {
	client, err := a.connectMaster()
	if err != nil {
		return nil, err
	}

	c := cluster.Cluster{
		ID: config.PoolID,
	}

	if err := client.AddCluster(c); err != nil {
		return nil, err
	}

	return a.GetCluster(c.ID)
}

// Removes an existing cluster
func (a *api) RemoveCluster(id string) error {
	client, err := a.connectMaster()
	if err != nil {
		return err
	}

	return client.RemoveCluster(id)
}

// Returns a list of Pools for a given cluster
func (a *api) GetPools(id string) (*pool.Pool, error) {
	client, err := a.connectMaster()
	if err != nil {
		return nil, err
	}

	return client.GetPools(id)
}

// Add a VirtualIP to a specific pool
func (a *api) AddVirtualIP(requestVirtualIP pool.VirtualIP) error {
	client, err := a.connectMaster()
	if err != nil {
		return err
	}

	return client.AddVirtualIP(requestVirtualIP)
}

// Add a VirtualIP to a specific pool
func (a *api) RemoveVirtualIP(requestVirtualIP pool.VirtualIP) error {
	client, err := a.connectMaster()
	if err != nil {
		return err
	}

	return client.RemoveVirtualIP(requestVirtualIP)
}
