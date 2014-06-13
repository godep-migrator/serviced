// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by a
// license that can be found in the LICENSE file.

package elasticsearch

import (
	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/rpc/agent"
	zkservice "github.com/zenoss/serviced/zzk/service"
)

func (this *ControlPlaneDao) GetServiceLogs(id string, logs *string) error {
	glog.V(3).Info("ControlPlaneDao.GetServiceLogs id=", id)
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	states, err := zkservice.LoadRunningServicesByService(conn, id)
	if err != nil {
		return err
	}
	if len(states) == 0 {
		glog.V(1).Info("Unable to find any running services for ", id)
		return nil
	}

	serviceState := states[0].GetServiceState()
	// FIXME: don't assume port is 4979
	endpoint := serviceState.HostIP + ":4979"
	agentClient, err := agent.NewClient(endpoint)
	if err != nil {
		glog.Errorf("could not create client to %s", endpoint)
		return err
	}
	defer agentClient.Close()
	if mylogs, err := agentClient.GetDockerLogs(serviceState.DockerID); err != nil {
		glog.Errorf("could not get docker logs from agent client: %s", err)
		return err
	} else {
		*logs = mylogs
	}
	return nil
}

func (this *ControlPlaneDao) GetServiceStateLogs(request dao.ServiceStateRequest, logs *string) error {
	/* TODO: This command does not support logs on other hosts */
	glog.V(3).Info("ControlPlaneDao.GetServiceStateLogs id=", request)
	conn, err := this.zclient.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	state, err := zkservice.LoadRunningService(conn, request.ServiceID, request.ServiceStateID)
	if err != nil {
		glog.Errorf("ControlPlaneDao.GetServiceStateLogs servicestate=%+v err=%s", state, err)
		return err
	}
	serviceState := state.GetServiceState()

	// FIXME: don't assume port is 4979
	endpoint := serviceState.HostIP + ":4979"
	agentClient, err := agent.NewClient(endpoint)
	if err != nil {
		glog.Errorf("could not create client to %s", endpoint)
		return err
	}
	defer agentClient.Close()
	if mylogs, err := agentClient.GetDockerLogs(serviceState.DockerID); err != nil {
		glog.Errorf("could not get docker logs from agent client: %s", err)
		return err
	} else {
		*logs = mylogs
	}
	return nil
}
