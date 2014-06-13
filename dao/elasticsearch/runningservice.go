// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by a
// license that can be found in the LICENSE file.

package elasticsearch

import (
	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/dao"
	zkservice "github.com/zenoss/serviced/zzk/service"
)

func (this *ControlPlaneDao) GetRunningServices(request dao.EntityRequest, services *[]*dao.RunningService) error {
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	*services, err = zkservice.LoadRunningServices(conn)
	return err
}

func (this *ControlPlaneDao) GetRunningServicesForHost(hostId string, services *[]*dao.RunningService) error {
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	*services, err = zkservice.LoadRunningServicesByHost(conn, hostId)
	return err
}

func (this *ControlPlaneDao) GetRunningServicesForService(serviceId string, services *[]*dao.RunningService) error {
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	*services, err = zkservice.LoadRunningServicesByService(conn, serviceId)
	return err
}

func (this *ControlPlaneDao) GetRunningService(request dao.ServiceStateRequest, running *dao.RunningService) error {
	glog.V(3).Infof("ControlPlaneDao.GetRunningService: request=%v", request)
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	running, err = zkservice.LoadRunningService(conn, request.ServiceID, request.ServiceStateID)
	return err
}

func (this *ControlPlaneDao) StopRunningInstance(request dao.HostServiceRequest, unused *int) error {
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	return zkservice.StopServiceInstance(conn, request.HostID, request.ServiceStateID)
}
