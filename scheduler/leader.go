package scheduler

import (
	"fmt"
	"time"

	"github.com/zenoss/glog"
	coordclient "github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/datastore"
	"github.com/zenoss/serviced/domain/addressassignment"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
	"github.com/zenoss/serviced/facade"
	"github.com/zenoss/serviced/zzk"
)

type leader struct {
	facade  *facade.Facade
	dao     dao.ControlPlane
	conn    coordclient.Connection
	context datastore.Context
}

// Lead is executed by the "leader" of the control plane cluster to handle its
// service/snapshot management responsibilities.
func Lead(facade *facade.Facade, dao dao.ControlPlane, conn coordclient.Connection, zkEvent <-chan coordclient.Event) {

	glog.V(0).Info("Entering Lead()!")
	defer glog.V(0).Info("Exiting Lead()!")
	shutdownmode := false
	leader := leader{facade: facade, dao: dao, conn: conn, context: datastore.Get()}
	for {
		if shutdownmode {
			glog.V(1).Info("Shutdown mode encountered.")
			break
		}
		time.Sleep(time.Second)
		func() error {
			select {
			case evt := <-zkEvent:
				// shut this thing down
				shutdownmode = true
				glog.V(0).Info("Got a zkevent, leaving lead: ", evt)
				return nil
			default:
				glog.V(0).Info("Processing leader duties")
				// passthru
			}

			go leader.watchSnapshots()
			leader.watchServices()
			return nil
		}()
	}
}

func snapShotName(volumeName string) string {
	format := "20060102-150405"
	loc := time.Now()
	utc := loc.UTC()
	return volumeName + "_" + utc.Format(format)
}

func (l *leader) watchSnapshots() {
	glog.V(0).Info("Leader waiting for snapshot requests")
	defer glog.V(0).Info("Leader finished waiting for snapshots")

	conn := l.conn
	cpDao := l.dao

	// watch for snapshots
	for {
		var snapshots []*zzk.Snapshot
		evt, err := zzk.LoadSnapshotsW(conn, &snapshots)
		if err != nil {
			glog.Errorf("Leader unable to watch snapshots: %s", err)
			return
		}
		for _, snapshot := range snapshots {
			if snapshot.Done() {
				continue
			}

			glog.V(1).Infof("Leader taking snapshot for request: %v", snapshot)
			var label string
			err := cpDao.LocalSnapshot(snapshot.ServiceID, &label)
			if err != nil {
				glog.V(1).Infof("Leader unable to take snapshot: %s", err)
			}
			(*snapshot).Label = label
			(*snapshot).Error = err
			zzk.UpdateSnapshot(conn, snapshot)
			glog.V(1).Infof("Leader finished taking snapshot for request: %+v", snapshot)
		}
		<-evt
	}
}

func (l *leader) watchServices() {
	conn := l.conn
	processing := make(map[string]interface{})
	done := make(chan string)

	// If this function exits shut down all subroutines
	shutdown := make(chan interface{})
	defer func() {
		glog.V(0).Infof("Leader shutting down children")
		close(shutdown)
	}()

	glog.V(0).Infof("Leader waiting for service changes")
	defer glog.V(1).Infof("Leader done waiting for service changes")
	for {
		var services []*service.Service
		evt, err := zzk.LoadServicesW(conn, &services)
		if err != nil {
			glog.Errorf("Leader could not watch services: %s", err)
			return
		}
		for _, s := range services {
			if _, ok := processing[s.Id]; !ok {
				processing[s.Id] = nil
				go l.watchService(shutdown, done, s.Id)
			}
		}

		for {
			select {
			case serviceID := <-done:
				// clean up any finished services
				glog.V(2).Infof("Leader cleaning up service: %s", serviceID)
				delete(processing, serviceID)
			case <-evt:
				// received an event on the node
				break
			}
		}
	}
}

func (l *leader) watchService(shutdown <-chan interface{}, done chan<- string, serviceID string) {
	conn := l.conn
	defer func() {
		glog.V(2).Infof("Leader done watching service %s", serviceID)
		done <- serviceID
	}()

	for {
		// watch for changes on the service
		var svc service.Service
		svcEvent, err := zzk.LoadServiceW(conn, &svc, serviceID)
		if err != nil {
			glog.Errorf("Leader unable to load service %s: %v", serviceID, err)
			return
		}

		// watch for changes on the service state; if node not exist return
		var states []*servicestate.ServiceState
		stateEvent, err := zzk.LoadServiceStatesW(conn, &states, serviceID)
		if err != nil {
			glog.Errorf("Leader unable to load service states for service %s: %v", serviceID, err)
			return
		}

		switch {
		case svc.DesiredState == service.SVCStop:
			shutdownServiceInstances(l.conn, states, len(states))
		case svc.DesiredState == service.SVCRun:
			l.updateServiceInstances(&svc, states)
		default:
			glog.Warningf("Unexpected desired state %d for service %s", svc.DesiredState, svc.Name)
		}

		select {
		case event := <-svcEvent:
			if event.Type == coordclient.EventNodeDeleted {
				glog.V(1).Infof("Shutting down due to node delete: %s", serviceID)
				shutdownServiceInstances(conn, states, len(states))
				return
			}
			glog.V(2).Infof("Service %s received event: %v", svc.Name, event)
		case event := <-stateEvent:
			glog.V(2).Infof("Service %s received child event: %v", svc.Name, event)
		case <-shutdown:
			glog.V(2).Infof("Leader stopping watch on %s", svc.Name)
			return
		}
	}
}

func (l *leader) updateServiceInstances(service *service.Service, serviceStates []*servicestate.ServiceState) error {
	//	var err error
	// pick services instances to start
	if len(serviceStates) < service.Instances {
		instancesToStart := service.Instances - len(serviceStates)
		glog.V(2).Infof("updateServiceInstances wants to start %d instances", instancesToStart)
		hosts, err := l.facade.FindHostsInPool(l.context, service.PoolId)
		if err != nil {
			glog.Errorf("Leader unable to acquire hosts for pool %s: %v", service.PoolId, err)
			return err
		}
		if len(hosts) == 0 {
			glog.Warningf("Pool %s has no hosts", service.PoolId)
			return nil
		}

		return l.startServiceInstances(service, hosts, instancesToStart)

	} else if len(serviceStates) > service.Instances {
		instancesToKill := len(serviceStates) - service.Instances
		glog.V(2).Infof("updateServiceInstances wants to kill %d instances", instancesToKill)
		shutdownServiceInstances(l.conn, serviceStates, instancesToKill)
	}
	return nil

}

// getFreeInstanceIds looks up running instances of this service and returns n
// unused instance ids.
// Note: getFreeInstanceIds does NOT validate that instance ids do not exceed
// max number of instances for the service. We're already doing that check in
// another, better place. It is guaranteed that either nil or n ids will be
// returned.
func getFreeInstanceIds(conn coordclient.Connection, svc *service.Service, n int) ([]int, error) {
	var (
		states []*servicestate.ServiceState
		ids    []int
	)
	// Look up existing instances
	if err := zzk.LoadServiceStates(conn, &states, svc.Id); err != nil {
		return nil, err
	}
	// Populate the used set
	used := make(map[int]interface{})
	for _, s := range states {
		used[s.InstanceId] = nil
	}
	// Find n unused ids
	for i := 0; len(ids) < n; i++ {
		if _, ok := used[i]; !ok {
			// Id is unused
			ids = append(ids, i)
		}
	}
	return ids, nil
}
func (l *leader) startServiceInstances(svc *service.Service, hosts []*host.Host, numToStart int) error {
	glog.V(1).Infof("Starting %d instances, choosing from %d hosts", numToStart, len(hosts))

	// Get numToStart free instance ids
	freeids, err := getFreeInstanceIds(l.conn, svc, numToStart)
	if err != nil {
		return err
	}

	hostPolicy := NewServiceHostPolicy(svc, l.dao)

	// Start up an instance per id
	for _, i := range freeids {
		servicehost, err := hostPolicy.SelectHost(hosts)
		if err != nil {
			return err
		}

		glog.V(2).Info("Selected host ", servicehost)
		serviceState, err := servicestate.BuildFromService(svc, servicehost.ID)
		if err != nil {
			glog.Errorf("Error creating ServiceState instance: %v", err)
			return err
		}

		serviceState.HostIp = servicehost.IPAddr
		serviceState.InstanceId = i
		err = zzk.AddServiceState(l.conn, serviceState)
		if err != nil {
			glog.Errorf("Leader unable to add service state: %v", err)
			return err
		}
		glog.V(2).Info("Started ", serviceState)
	}
	return nil
}

func shutdownServiceInstances(conn coordclient.Connection, serviceStates []*servicestate.ServiceState, numToKill int) {
	glog.V(1).Infof("Stopping %d instances from %d total", numToKill, len(serviceStates))
	for i := 0; i < numToKill; i++ {
		glog.V(2).Infof("Killing host service state %s:%s\n", serviceStates[i].HostId, serviceStates[i].Id)
		serviceStates[i].Terminated = time.Date(2, time.January, 1, 0, 0, 0, 0, time.UTC)
		err := zzk.TerminateHostService(conn, serviceStates[i].HostId, serviceStates[i].Id)
		if err != nil {
			glog.Warningf("%s:%s wouldn't die", serviceStates[i].HostId, serviceStates[i].Id)
		}
	}
}

// selectPoolHostForService chooses a host from the pool for the specified service. If the service
// has an address assignment the host will already be selected. If not the host with the least amount
// of memory committed to running containers will be chosen.
func (l *leader) selectPoolHostForService(s *service.Service, hosts []*host.Host) (*host.Host, error) {
	var hostid string
	for _, ep := range s.Endpoints {
		if ep.AddressAssignment != (addressassignment.AddressAssignment{}) {
			hostid = ep.AddressAssignment.HostID
			break
		}
	}

	if hostid != "" {
		return poolHostFromAddressAssignments(hostid, hosts)
	}

	return NewServiceHostPolicy(s, l.dao).SelectHost(hosts)
}

// poolHostFromAddressAssignments determines the pool host for the service from its address assignment(s).
func poolHostFromAddressAssignments(hostid string, hosts []*host.Host) (*host.Host, error) {
	// ensure the assigned host is in the pool
	for _, h := range hosts {
		if h.ID == hostid {
			return h, nil
		}
	}

	return nil, fmt.Errorf("assigned host is not in pool")
}
