package service

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

const (
	zkService = "/services"
)

func servicepath(nodes ...string) string {
	p := append([]string{zkService}, nodes...)
	return path.Join(p...)
}

type LookupHosts func(poolID string) ([]*host.Host, error)

func Listen(conn client.Connection, lh LookupHosts) {
	shutdown := make(chan interface{})
	defer func() {
		glog.Info("Shutting down all goroutines")
		close(shutdown)
	}()

	if exists, err := conn.Exists(servicepath()); err != nil {
		glog.Error("Unable to look up service path on zookeeper: ", err)
		return
	} else if exists {
		// pass
	} else if err := conn.CreateDir(servicepath()); err != nil {
		glog.Error("Unabel to create service path on zookeeper: ", err)
		return
	}

	var (
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

	for {
		serviceIDs, event, err := conn.ChildrenW(servicepath())
		if err != nil {
			glog.Error("Unable to watch services: ", err)
			return
		}

		for _, serviceID := range serviceIDs {
			if _, ok := processing[serviceID]; !ok {
				glog.V(1).Info("Spawning a listener for service ", serviceID)
				processing[serviceID] = nil
				go ListenService(conn, shutdown, done, serviceID, lh)
			}
		}

		select {
		case e := <-event:
			glog.V(2).Infof("Received event: %v", e)
		case serviceID := <-done:
			glog.V(2).Info("Cleaning up service ", serviceID)
			delete(processing, serviceID)
		}
	}
}

func ListenService(conn client.Connection, shutdown <-chan interface{}, done chan<- string, serviceID string, lh LookupHosts) {
	defer func() {
		glog.V(2).Info("Shutting down listener for service ", serviceID)
		done <- serviceID
	}()

	for {
		var svc service.Service
		serviceEvent, err := conn.GetW(servicepath(serviceID), &svc)
		if err != nil {
			glog.Errorf("Could not load service %s: %s", serviceID, err)
			return
		}

		stateIDs, stateEvent, err := conn.ChildrenW(servicepath(serviceID))
		if err != nil {
			glog.Errorf("Could not watch service states for service %s (%s): %s", svc.Name, svc.Id, err)
			return
		}

		// synchronize running states
		glog.V(2).Infof("Listening on service %s (%s)", svc.Name, svc.Id)
		switch svc.DesiredState {
		case service.SVCRun:
			SyncServiceInstances(conn, &svc, stateIDs, lh)
		case service.SVCStop:
			StopServiceInstances(conn, &svc, stateIDs...)
		default:
			glog.Warningf("Unknown service state %d for service %s (%s)", svc.DesiredState, svc.Name, svc.Id)
		}

		select {
		case e := <-serviceEvent:
			if e.Type == client.EventNodeDeleted {
				glog.V(1).Infof("Shutting down due to node delete %s (%s)", svc.Name, svc.Id)
				StopServiceInstances(conn, &svc, stateIDs...)
				return
			}
			glog.V(2).Infof("Service %s (%s) receieved event: %v", svc.Name, svc.Id, e)
		case e := <-stateEvent:
			glog.V(2).Infof("Service %s (%s) receieved state event: %v", svc.Name, svc.Id, e)
		case <-shutdown:
			glog.V(1).Infof("Service %s (%s) receieved signal to shutdown", svc.Name, svc.Id)
			StopServiceInstances(conn, &svc, stateIDs...)
			return
		}
	}
}

func StartServiceInstances(conn client.Connection, service *service.Service, hosts []*host.Host, instanceIDs ...int) {
	policy := host.NewServiceHostPolicy(service, &ZKHostInfo{conn})
	for _, i := range instanceIDs {
		host, err := policy.Select(hosts)
		if err != nil {
			glog.Errorf("Error acquiring host policy for service %s: %s", service.Id, err)
			return
		}
		glog.V(2).Info("Selected host ", host.ID)
		state, err := servicestate.BuildFromService(service, host.ID)
		if err != nil {
			glog.Errorf("Error creating service instance for service %s (%s): %s", service.Name, service.Id, err)
			return
		}

		state.HostIP = host.IPAddr
		state.InstanceID = i
		if err := conn.Create(servicepath(state.ServiceID, state.Id), state); err != nil {
			glog.Errorf("Could not add service instance %s: %s", state.Id, err)
			return
		}

		if err := conn.Create(hostpath(state.HostID, state.Id), NewHostState(state)); err != nil {
			glog.Errorf("Could not add service instance %s: %s", state.Id, err)
			conn.Delete(servicepath(state.ServiceID, state.Id))
			return
		}

		glog.V(2).Infof("Starting service instance %s via host %s ", state.Id, state.HostID)
	}
}

func StopServiceInstances(conn client.Connection, svc *service.Service, stateIDs ...string) {
	for _, ssID := range stateIDs {
		var state servicestate.ServiceState
		if err := conn.Get(servicepath(svc.Id, ssID), &state); err != nil {
			glog.Errorf("Could not get service instance %s: %s", ssID, err)
			return
		}

		var hs HostState
		if err := conn.Get(hostpath(state.HostID, ssID), &hs); err != nil {
			glog.Errorf("Could not get service instance %s via host %s: %s", state.Id, state.HostID, err)
			return
		}

		glog.V(2).Infof("Stopping service instance %s via host %s", state.Id, state.HostID)
		hs.DesiredState = service.SVCStop
		if err := conn.Set(hostpath(state.HostID, hs.ID), &hs); err != nil {
			glog.Warningf("Service instance %s won't die", state.Id)
		}
	}
}

func SyncServiceInstances(conn client.Connection, service *service.Service, stateIDs []string, lh LookupHosts) {
	netInstances := service.Instances - len(stateIDs)

	if netInstances > 0 {
		// find the hosts
		hosts, err := lh(service.PoolID)
		if err != nil {
			glog.Errorf("Could not lookup hosts for service %s (%s) with pool ID %s: %s", service.Name, service.Id, service.PoolID, err)
			return
		}

		// find the free instance ids
		used := make(map[int]interface{})
		for _, ssID := range stateIDs {
			var state servicestate.ServiceState
			if err := conn.Get(servicepath(service.Id, ssID), &state); err != nil {
				glog.Errorf("Could not get service state %s: %s", ssID, err)
				return
			}
			used[state.InstanceID] = nil
		}
		var instanceIDs []int
		for i := 0; len(instanceIDs) < netInstances; i++ {
			if _, ok := used[i]; !ok {
				instanceIDs = append(instanceIDs, i)
			}
		}

		glog.V(1).Infof("Starting up %d services for %s (%s)", netInstances, service.Name, service.Id)
		StartServiceInstances(conn, service, hosts, instanceIDs...)
	} else if netInstances < 0 {
		glog.V(1).Infof("Shutting down %d services for %s (%s)", netInstances, service.Name, service.Id)
		StopServiceInstances(conn, service, stateIDs[:-netInstances]...)
	}
}