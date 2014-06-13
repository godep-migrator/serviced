package service

import (
	"fmt"
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
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

type ServiceHandler interface {
	FindHostsInPool(poolID string) ([]*host.Host, error)
	SelectHost(service *service.Service, hosts []*host.Host, policy host.HostPolicy) (*host.Host, error)
}

type ServiceListener struct {
	conn    client.Connection
	handler ServiceHandler
}

func NewServiceListener(conn client.Connection, handler ServiceHandler) *ServiceListener {
	return &ServiceListener{conn, handler}
}

func (l *ServiceListener) Listen() {
	var (
		shutdown   = make(chan interface{})
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

	defer func() {
		glog.Info("Shutting down all goroutines")
		close(shutdown)
		for len(processing) > 0 {
			delete(processing, <-done)
		}
	}()

	if exists, err := l.conn.Exists(servicepath()); err != nil {
		glog.Error("Unable to look up service path on zookeeper: ", err)
		return
	} else if exists {
		// pass
	} else if err := l.conn.CreateDir(servicepath()); err != nil {
		glog.Error("Unable to create service path on zookeeper: ", err)
		return
	}

	for {
		serviceIDs, event, err := l.conn.ChildrenW(servicepath())
		if err != nil {
			glog.Error("Unable to watch services: ", err)
			return
		}

		for _, serviceID := range serviceIDs {
			if _, ok := processing[serviceID]; !ok {
				glog.V(1).Info("Spawning a listener for service ", serviceID)
				processing[serviceID] = nil
				go l.listenService(shutdown, done, serviceID)
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

func (l *ServiceListener) listenService(shutdown <-chan interface{}, done chan<- string, serviceID string) {
	defer func() {
		glog.V(2).Info("Shutting down listener for service ", serviceID)
		done <- serviceID
	}()

	for {
		var svc service.Service
		serviceEvent, err := l.conn.GetW(servicepath(serviceID), &svc)
		if err != nil {
			glog.Errorf("Could not load service %s: %s", serviceID, err)
			return
		}

		stateIDs, stateEvent, err := l.conn.ChildrenW(servicepath(serviceID))
		if err != nil {
			glog.Errorf("Could not watch service states for service %s (%s): %s", svc.Name, svc.Id, err)
			return
		}

		// synchronize running states
		glog.V(2).Infof("Listening on service %s (%s)", svc.Name, svc.Id)
		switch svc.DesiredState {
		case service.SVCRun:
			l.syncServiceInstances(&svc, stateIDs)
		case service.SVCStop:
			l.stopServiceInstances(&svc, stateIDs)
		default:
			glog.Warningf("Unknown service state %d for service %s (%s)", svc.DesiredState, svc.Name, svc.Id)
		}

		select {
		case e := <-serviceEvent:
			if e.Type == client.EventNodeDeleted {
				glog.V(1).Infof("Shutting down due to node delete %s (%s)", svc.Name, svc.Id)
				l.stopServiceInstances(&svc, stateIDs)
				return
			}
			glog.V(2).Infof("Service %s (%s) receieved event: %v", svc.Name, svc.Id, e)
		case e := <-stateEvent:
			glog.V(2).Infof("Service %s (%s) receieved state event: %v", svc.Name, svc.Id, e)
		case <-shutdown:
			glog.V(1).Infof("Service %s (%s) receieved signal to shutdown", svc.Name, svc.Id)
			l.stopServiceInstances(&svc, stateIDs)
			return
		}
	}
}

func (l *ServiceListener) startServiceInstances(svc *service.Service, hosts []*host.Host, instanceIDs []int) {
	policy := host.NewServiceHostPolicy(svc, l)
	for _, i := range instanceIDs {
		host, err := l.handler.SelectHost(svc, hosts, policy)
		if err != nil {
			glog.Errorf("Error acquiring host policy for service %s: %s", svc.Id, err)
			return
		}
		glog.V(2).Info("Selected host ", host.ID)
		state, err := servicestate.BuildFromService(svc, host.ID)
		if err != nil {
			glog.Errorf("Error creating service instance for service %s (%s): %s", svc.Name, svc.Id, err)
			return
		}

		state.HostIP = host.IPAddr
		state.InstanceID = i
		if err := l.conn.Create(servicepath(state.ServiceID, state.Id), state); err != nil {
			glog.Errorf("Could not add service instance %s: %s", state.Id, err)
			return
		}

		if err := l.conn.Create(hostpath(state.HostID, state.Id), NewHostState(state)); err != nil {
			glog.Errorf("Could not add service instance %s: %s", state.Id, err)
			if err := l.conn.Delete(servicepath(state.ServiceID, state.Id)); err != nil {
				glog.Warningf("Could not remove service instance %s: %s", state.Id, err)
			}
			return
		}

		glog.V(2).Infof("Starting service instance %s via host %s ", state.Id, state.HostID)
	}
}

func (l *ServiceListener) stopServiceInstances(svc *service.Service, stateIDs []string) {
	for _, ssID := range stateIDs {
		var state servicestate.ServiceState
		if err := l.conn.Get(servicepath(svc.Id, ssID), &state); err != nil {
			glog.Errorf("Could retrieve service instance %s: %s", ssID, err)
			return
		}
		if err := StopServiceInstance(l.conn, state.HostID, state.Id); err != nil {
			glog.Warningf("Service instance %s won't die", state.Id)
		}
	}
}

func (l *ServiceListener) syncServiceInstances(svc *service.Service, stateIDs []string) {
	netInstances := svc.Instances - len(stateIDs)

	if netInstances > 0 {
		// find the hosts
		hosts, err := l.handler.FindHostsInPool(svc.PoolID)
		if err != nil {
			glog.Errorf("Could not lookup hosts for service %s (%s) with pool ID %s: %s", svc.Name, svc.Id, svc.PoolID, err)
			return
		}

		// find the free instance ids
		used := make(map[int]interface{})
		for _, ssID := range stateIDs {
			var state servicestate.ServiceState
			if err := l.conn.Get(servicepath(svc.Id, ssID), &state); err != nil {
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

		glog.V(1).Infof("Starting up %d services for %s (%s)", netInstances, svc.Name, svc.Id)
		l.startServiceInstances(svc, hosts, instanceIDs)
	} else if netInstances < 0 {
		glog.V(1).Infof("Shutting down %d services for %s (%s)", netInstances, svc.Name, svc.Id)
		l.stopServiceInstances(svc, stateIDs[:-netInstances])
	}
}

func (l *ServiceListener) ServicesOnHost(host *host.Host) ([]*dao.RunningService, error) {
	return LoadRunningServicesByHost(l.conn, host.ID)
}

func UpdateService(conn client.Connection, svc *service.Service) error {
	if svc.Id == "" {
		return fmt.Errorf("service id required")
	}
	spath := servicepath(svc.Id)
	if exists, err := conn.Exists(spath); err != nil {
		return err
	} else if !exists {
		return conn.Create(spath, svc)
	}

	return conn.Set(spath, svc)
}

func RemoveService(conn client.Connection, id string) error {
	if id == "" {
		return fmt.Errorf("service id required")
	}
	return conn.Delete(servicepath(id))
}