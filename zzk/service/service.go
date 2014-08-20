// Copyright 2014 The Serviced Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"path"
	"sort"
	"sync"

	"github.com/control-center/serviced/coordinator/client"
	"github.com/control-center/serviced/dao"
	"github.com/control-center/serviced/domain/host"
	"github.com/control-center/serviced/domain/service"
	"github.com/control-center/serviced/domain/servicestate"
	"github.com/control-center/serviced/utils"
	"github.com/control-center/serviced/zzk"
	"github.com/zenoss/glog"
)

const (
	zkService = "/services"
)

func servicepath(nodes ...string) string {
	p := append([]string{zkService}, nodes...)
	return path.Join(p...)
}

type instances []dao.RunningService

func (inst instances) Len() int           { return len(inst) }
func (inst instances) Less(i, j int) bool { return inst[i].InstanceID < inst[j].InstanceID }
func (inst instances) Swap(i, j int)      { inst[i], inst[j] = inst[j], inst[i] }

// ServiceNode is the zookeeper client Node for services
type ServiceNode struct {
	*service.Service
	version interface{}
}

// ID implements zzk.Node
func (node *ServiceNode) GetID() string {
	return node.ID
}

// Create implements zzk.Node
func (node *ServiceNode) Create(conn client.Connection) error {
	return UpdateService(conn, node.Service)
}

// Update implements zzk.Node
func (node *ServiceNode) Update(conn client.Connection) error {
	return UpdateService(conn, node.Service)
}

// Version implements client.Node
func (node *ServiceNode) Version() interface{} { return node.version }

// SetVersion implements client.Node
func (node *ServiceNode) SetVersion(version interface{}) { node.version = version }

// ServiceHandler handles all non-zookeeper interactions required by the service
type ServiceHandler interface {
	SelectHost(*service.Service) (*host.Host, error)
}

// ServiceListener is the listener for /services
type ServiceListener struct {
	sync.Mutex
	conn    client.Connection
	handler ServiceHandler
}

// NewServiceListener instantiates a new ServiceListener
func NewServiceListener(conn client.Connection, handler ServiceHandler) *ServiceListener {
	return &ServiceListener{conn: conn, handler: handler}
}

// GetConnection implements zzk.Listener
func (l *ServiceListener) GetConnection() client.Connection { return l.conn }

// GetPath implements zzk.Listener
func (l *ServiceListener) GetPath(nodes ...string) string { return servicepath(nodes...) }

// Ready implements zzk.Listener
func (l *ServiceListener) Ready() (err error) { return }

// Done implements zzk.Listener
func (l *ServiceListener) Done() { return }

// Spawn watches a service and syncs the number of running instances
func (l *ServiceListener) Spawn(shutdown <-chan interface{}, serviceID string) {
	for {
		var svc service.Service
		serviceEvent, err := l.conn.GetW(l.GetPath(serviceID), &ServiceNode{Service: &svc})
		if err != nil {
			glog.Errorf("Could not load service %s: %s", serviceID, err)
			return
		}

		_, stateEvent, err := l.conn.ChildrenW(l.GetPath(serviceID))
		if err != nil {
			glog.Errorf("Could not load service states for %s: %s", serviceID, err)
			return
		}

		rss, err := LoadRunningServicesByService(l.conn, svc.ID)
		if err != nil {
			glog.Errorf("Could not load states for service %s (%s): %s", svc.Name, svc.ID, err)
		}

		// Should the service be running at all?
		switch svc.DesiredState {
		case service.SVCStop:
			l.stop(rss)
		case service.SVCRun:
			l.sync(&svc, rss)
		case service.SVCPause:
			l.pause(rss)
		default:
			glog.Warningf("Unexpected desired state %d for service %s (%s)", svc.DesiredState, svc.Name, svc.ID)
		}

		select {
		case e := <-serviceEvent:
			if e.Type == client.EventNodeDeleted {
				glog.V(2).Infof("Shutting down service %s (%s) due to node delete", svc.Name, svc.ID)
				l.stop(rss)
				return
			}
			glog.V(2).Infof("Service %s (%s) received event: %v", svc.Name, svc.ID, e)
		case e := <-stateEvent:
			if e.Type == client.EventNodeDeleted {
				glog.V(2).Infof("Shutting down service %s (%s) due to node delete", svc.Name, svc.ID)
				l.stop(rss)
				return
			}
			glog.V(2).Infof("Service %s (%s) received event: %v", svc.Name, svc.ID, e)
		case <-shutdown:
			glog.V(2).Infof("Leader stopping watch for %s (%s)", svc.Name, svc.ID)
			l.stop(rss)
			return
		}
	}
}

func (l *ServiceListener) sync(svc *service.Service, rss []dao.RunningService) {
	// only one service can start and stop service instances at a time
	l.Lock()
	defer l.Unlock()

	// sort running services by instance ID, so that you stop instances by the
	// lowest instance ID first and start instances with the greatest instance
	// ID last.
	sort.Sort(instances(rss))

	// resume any paused running services
	for _, state := range rss {
		// resumeInstance updates the service state ONLY if it has a PAUSED DesiredState
		if err := resumeInstance(l.conn, state.HostID, state.ID); err != nil {
			glog.Warningf("Could not resume paused service instance %s (%s) for service %s on host %s: %s", state.ID, state.Name, state.ServiceID, state.HostID, err)
		}
	}

	// if the service has a change option for restart all on changed, stop all
	// instances and wait for the nodes to stop.  Once all service instances
	// have been stopped (deleted), then go ahead and start the instances back
	// up.
	if count := len(rss); count > 0 && count != svc.Instances && utils.StringInSlice("restartAllOnInstanceChanged", svc.ChangeOptions) {
		svc.Instances = 0 // NOTE: this will not update the node in zk or elastic
	}

	// netInstances is the difference between the number of instances that
	// should be running, as described by the service from the number of
	// instances that are currently running
	netInstances := svc.Instances - len(rss)

	if netInstances > 0 {
		// the number of running instances is *less* than the number of
		// instances that need to be running, so schedule instances to start
		glog.V(1).Infof("Starting %d instances of service %s (%s)", netInstances, svc.Name, svc.ID)
		var (
			last        = 0
			instanceIDs = make([]int, netInstances)
		)

		// Find which instances IDs are being unused and add those instances
		// first.  All SERVICES must have an instance ID of 0, if instance ID
		// zero dies for whatever reason, then the service must schedule
		// another 0-id instance to take its place.
		j := 0
		for i := range instanceIDs {
			for j < len(rss) && last == rss[j].InstanceID {
				// if instance ID exists, then keep searching the list for
				// the next unique instance ID
				last += 1
				j += 1
			}
			instanceIDs[i] = last
			last += 1
		}

		l.start(svc, instanceIDs)
	} else if netInstances = -netInstances; netInstances > 0 {
		// the number of running instances is *greater* than the number of
		// instances that need to be running, so schedule instances to stop of
		// the highest instance IDs.
		glog.V(1).Infof("Stopping %d of %d instances of service %s (%s)", netInstances, len(rss), svc.Name, svc.ID)
		l.stop(rss[svc.Instances:])
	}
}

func (l *ServiceListener) start(svc *service.Service, instanceIDs []int) {
	for _, i := range instanceIDs {
		host, err := l.handler.SelectHost(svc)
		if err != nil {
			glog.Warningf("Could not assign a host to service %s (%s): %s", svc.Name, svc.ID, err)
			continue
		}
		state, err := servicestate.BuildFromService(svc, host.ID)
		if err != nil {
			glog.Warningf("Error creating service state for service %s (%s): %s", svc.Name, svc.ID, err)
			continue
		}
		state.HostIP = host.IPAddr
		state.InstanceID = i
		if err := addInstance(l.conn, state); err != nil {
			glog.Warningf("Could not add service instance %s for service %s (%s): %s", state.ID, svc.Name, svc.ID, err)
			continue
		}
		glog.V(2).Infof("Starting service instance %s for service %s (%s) on host %s", state.ID, svc.Name, svc.ID, host.ID)
	}
}

func (l *ServiceListener) stop(rss []dao.RunningService) {
	for _, state := range rss {
		if err := StopServiceInstance(l.conn, state.HostID, state.ID); err != nil {
			glog.Warningf("Service instance %s (%s) from service %s won't die: %s", state.ID, state.Name, state.ServiceID, err)
			continue
		}
		glog.V(2).Infof("Stopping service instance %s (%s) for service %s on host %s", state.ID, state.Name, state.ServiceID, state.HostID)
	}
}

func (l *ServiceListener) pause(rss []dao.RunningService) {
	for _, state := range rss {
		// pauseInstance updates the service state ONLY if it has a RUN DesiredState
		if err := pauseInstance(l.conn, state.HostID, state.ID); err != nil {
			glog.Warningf("Could not pause service instance %s (%s) for service %s: %s", state.ID, state.Name, state.ServiceID, err)
			continue
		}
		glog.V(2).Infof("Pausing service instance %s (%s) for service %s on host %s", state.ID, state.Name, state.ServiceID, state.HostID)
	}
}

// StartService schedules a service to start
func StartService(conn client.Connection, serviceID string) error {
	glog.Infof("Scheduling service %s to start", serviceID)
	var node ServiceNode
	path := servicepath(serviceID)

	if err := conn.Get(path, &node); err != nil {
		return err
	}
	node.Service.DesiredState = service.SVCRun
	return conn.Set(path, &node)
}

// StopService schedules a service to stop
func StopService(conn client.Connection, serviceID string) error {
	glog.Infof("Scheduling service %s to stop", serviceID)
	var node ServiceNode
	path := servicepath(serviceID)

	if err := conn.Get(path, &node); err != nil {
		return err
	}
	node.Service.DesiredState = service.SVCStop
	return conn.Set(path, &node)
}

// SyncServices synchronizes all services into zookeeper
func SyncServices(conn client.Connection, services []service.Service) error {
	nodes := make([]zzk.Node, len(services))
	for i := range services {
		nodes[i] = &ServiceNode{Service: &services[i]}
	}
	return zzk.Sync(conn, nodes, servicepath())
}

// UpdateService updates a service node if it exists, otherwise creates it
func UpdateService(conn client.Connection, svc *service.Service) error {
	var node ServiceNode
	spath := servicepath(svc.ID)

	// For some reason you can't just create the node with the service data
	// already set.  Trust me, I tried.  It was very aggravating.
	if err := conn.Get(spath, &node); err != nil {
		if err := conn.Create(spath, &node); err != nil {
			glog.Errorf("Error trying to create node at %s: %s", spath, err)
		}
	}
	node.Service = svc
	return conn.Set(spath, &node)
}

// RemoveServices stop any running services and deletes an existing service
func RemoveService(cancel <-chan interface{}, conn client.Connection, serviceID string) error {
	// Check if the path exists
	if exists, err := zzk.PathExists(conn, servicepath(serviceID)); err != nil {
		return err
	} else if !exists {
		return nil
	}

	// If it exists, stop the service
	if err := StopService(conn, serviceID); err != nil {
		return err
	}

	// Wait for there to be no running states
	for {
		children, event, err := conn.ChildrenW(servicepath(serviceID))
		if err != nil {
			return err
		}

		if len(children) == 0 {
			break
		}

		select {
		case <-event:
			// pass
		case <-cancel:
			glog.Infof("Gave up deleting service %s with %d children", serviceID, len(children))
			return zzk.ErrShutdown
		}
	}

	// Delete the service
	return conn.Delete(servicepath(serviceID))
}
