// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by a
// license that can be found in the LICENSE file.

package facade

import (
	"github.com/zenoss/serviced/datastore"
	"github.com/zenoss/serviced/datastore/elastic"
	"github.com/zenoss/serviced/domain/addressassignment"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/pool"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/serviceconfigfile"
	"github.com/zenoss/serviced/domain/servicestate"
	"github.com/zenoss/serviced/domain/servicetemplate"
	"github.com/zenoss/serviced/domain/user"
	"github.com/zenoss/serviced/zzk"
	gocheck "gopkg.in/check.v1"
)

//FacadeTest used for running tests where a facade type is needed.
type FacadeTest struct {
	elastic.ElasticTest
	CTX    datastore.Context
	Facade *Facade
}

//SetUpSuite sets up test suite
func (ft *FacadeTest) SetUpSuite(c *gocheck.C) {
	//set up index and mappings before setting up elastic
	ft.Index = "controlplane"
	if ft.Mappings == nil {
		ft.Mappings = make([]elastic.Mapping, 0)
	}
	ft.Mappings = append(ft.Mappings, host.MAPPING)
	ft.Mappings = append(ft.Mappings, pool.MAPPING)
	ft.Mappings = append(ft.Mappings, service.MAPPING)
	ft.Mappings = append(ft.Mappings, servicetemplate.MAPPING)
	ft.Mappings = append(ft.Mappings, addressassignment.MAPPING)
	ft.Mappings = append(ft.Mappings, serviceconfigfile.MAPPING)
	ft.Mappings = append(ft.Mappings, user.MAPPING)

	ft.ElasticTest.SetUpSuite(c)
	datastore.Register(ft.Driver())
	ft.CTX = datastore.Get()
	//empty zkdao will not work but tests shouldn't call this anyway.
	ft.Facade = New(&zzk.ZkDao{}, "localhost:5000")

	//mock out ZK calls to no ops
	zkAPI = func(zkdao *zzk.ZkDao) zkfuncs { return &zkMock{} }
}

type zkMock struct {
}

func (z *zkMock) updateService(svc *service.Service) error {
	return nil
}

func (z *zkMock) removeService(id string) error {
	return nil
}

func (z *zkMock) getSvcStates(serviceStates *[]*servicestate.ServiceState, serviceIds ...string) error {
	return nil
}

func (z *zkMock) RemoveHost(hostID string) error {
	return nil
}

func (z *zkMock) RegisterHost(hostID string) error {
	return nil
}

func (z *zkMock) UnregisterHost(hostID string) error {
	return nil
}
