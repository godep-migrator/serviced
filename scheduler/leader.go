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
	"github.com/zenoss/serviced/facade"
	zkservice "github.com/zenoss/serviced/zzk/service"
	zksnapshot "github.com/zenoss/serviced/zzk/snapshot"
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

			// creates a listener for snapshots with a function call to take
			// snapshots and return the label and error message
			go zksnapshot.Listen(conn, func(serviceID string) (string, error) {
				var label string
				err := dao.TakeSnapshot(serviceID, &label)
				return label, err
			})

			// creates a listener for services with a func call to find hosts
			// within a pool
			go zkservice.Listen(conn, func(poolID string) ([]*host.Host, error) {
				return leader.facade.FindHostsInPool(leader.context, poolID)
			})

			return nil
		}()
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
