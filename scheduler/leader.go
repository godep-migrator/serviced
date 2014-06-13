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
	zkService "github.com/zenoss/serviced/zzk/service"
	zkSnapshot "github.com/zenoss/serviced/zzk/snapshot"
	zkVirtualIP "github.com/zenoss/serviced/zzk/virtualips"
)

type leader struct {
	facade  *facade.Facade
	dao     dao.ControlPlane
	conn    coordclient.Connection
	context datastore.Context
}

// Lead is executed by the "leader" of the control plane cluster to handle its management responsibilities of:
//    services
//    snapshots
//    virtual IPs
func Lead(facade *facade.Facade, dao dao.ControlPlane, conn coordclient.Connection, zkEvent <-chan coordclient.Event) {
	glog.V(0).Info("Entering Lead()!")
	defer glog.V(0).Info("Exiting Lead()!")

	allPools, err := facade.GetResourcePools(datastore.Get())
	if err != nil {
		glog.Error(err)
		return
	} else if allPools == nil || len(allPools) == 0 {
		glog.Error("no resource pools found")
		return
	}

	leader := leader{facade: facade, dao: dao, conn: conn, context: datastore.Get()}
	for _, aPool := range allPools {
		// TODO: Support non default pools
		// Currently, only the default pool gets a leader
		if aPool.ID != "default" {
			glog.Warningf("Non default pool: %v (not currently supported)", aPool.ID)
			continue
		}

		leader.init(aPool.ID, zkEvent)
	}
}

func (l *leader) init(poolID string, zkEvent <-chan coordclient.Event) {
	defer func() {
		glog.V(1).Info("Entering Shutdown mode")
	}()

	for {
		select {
		case <-zkEvent:
			// shut down!
			return
		case <-time.After(time.Second):
			glog.V(0).Info("Processing Leader Duties")
		}

		// start the snapshot listener
		go zkSnapshot.NewSnapshotListener(l.conn, l).Listen()

		// start virtual ip synchronization
		go l.syncVirtualIPs(poolID)

		// start the service listener
		zkService.NewServiceListener(l.conn, l).Listen()
	}
}

func (l *leader) syncVirtualIPs(poolID string) {
	// TODO: currently just checking the pool every 10 seconds.  Maybe change
	// this to use a zookeeper listener instead?
	for {
		select {
		case <-time.After(10 * time.Second):
			pool, err := l.facade.GetResourcePool(l.context, poolID)
			if err != nil {
				glog.Errorf("Could not load resource pool %s: %s", poolID, err)
				return
			} else if pool == nil {
				glog.Errorf("Pool %s not found", poolID)
				return
			}

			if err := zkVirtualIP.Sync(l.conn, pool.VirtualIPs); err != nil {
				glog.Errorf("Could not sync virtual IP for pool %s: %s", poolID, err)
			}
		}
	}
}

func (l *leader) TakeSnapshot(serviceID string) (string, error) {
	var label string
	err := l.dao.TakeSnapshot(serviceID, &label)
	return label, err
}

func (l *leader) FindHostsInPool(poolID string) ([]*host.Host, error) {
	return l.facade.FindHostsInPool(l.context, poolID)
}

func (l *leader) SelectHost(svc *service.Service, hosts []*host.Host, policy host.HostPolicy) (*host.Host, error) {
	var (
		assignmentType string
		ipAddr         string
		hostID         string
	)

	for _, endpoint := range svc.Endpoints {
		if endpoint.AddressAssignment != (addressassignment.AddressAssignment{}) {
			assignmentType = endpoint.AddressAssignment.AssignmentType
			ipAddr = endpoint.AddressAssignment.IPAddr
			hostID = endpoint.AddressAssignment.HostID
			break
		}
	}

	if assignmentType == addressassignment.Virtual {
		// populate hostid
		var err error
		if hostID, err = zkVirtualIP.GetHost(l.conn, ipAddr); err != nil {
			return nil, err
		}
	}
	if hostID != "" {
		for _, h := range hosts {
			if h.ID == hostID {
				return h, nil
			}
		}
		return nil, fmt.Errorf("assigned host not in pool")
	}

	return policy.Select(hosts)
}
