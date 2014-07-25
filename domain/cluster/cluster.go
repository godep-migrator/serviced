// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/control-center/serviced/domain"

	"reflect"
	"sort"
	"time"
)

// Cluster
type Cluster struct {
	ID          string         // Unique identifier for the cluster
	Description string         // Description of the cluster
	Pools       []ResourcePool // All pools contained within this a cluster
}

// Equal returns true if two clusters are equal
func (a *Cluster) Equals(b *Cluster) bool {
	if a.ID != b.ID {
		return false
	}
	if a.Description != b.Description {
		return false
	}
	if a.Pools != b.Pools {
		return false
	}

	return true
}

// New creates new ResourcePool
func New(id string) *Cluster {
	cluster := &Cluster{}
	cluster.ID = id

	return cluster
}
