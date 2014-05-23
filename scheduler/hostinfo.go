package scheduler

import (
	"container/heap"
	"errors"
	"sync"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	zkservice "github.com/zenoss/serviced/zzk/service"
)

// HostInfo provides methods for getting host information from the dao or
// otherwise. It's a separate interface for the sake of testing.
type HostInfo interface {
	AvailableRAM(*host.Host, chan *hostitem, <-chan bool)
	ServicesOnHost(*host.Host) []*dao.RunningService
}

type ZKHostInfo struct {
	conn client.Connection
}

func (hi *ZKHostInfo) AvailableRAM(host *host.Host, result chan *hostitem, done <-chan bool) {
	rss, err := zkservice.LoadRunningServicesByHost(hi.conn, host.ID)
	if err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", host.ID, err)
		return // this host won't be scheduled
	}

	var totalRAM uint64
	for _, rs := range rss {
		totalRAM += rs.CommittedRAM
	}

	result <- &hostitem{host, host.Memory - cr, -1}
}

func (hi *ZKHostInfo) ServicesOnHost(host *host.Host) []*dao.RunningService {
	if rss, err := zkservice.LoadRunningServicesByHost(hi.conn, host.ID); err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", h.ID, err)
	}
	return rss
}

type DAOHostInfo struct {
	dao dao.ControlPlane
}

func (hi *DAOHostInfo) ServicesOnHost(h *host.Host) []*dao.RunningService {
	rss := []*dao.RunningService{}
	if err := hi.dao.GetRunningServicesForHost(h.ID, &rss); err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", h.ID, err)
	}
	return rss
}

// AvailableRAM computes the amount of RAM available on a given host by
// subtracting the sum of the RAM commitments of each of its running services
// from its total memory.
func (hi *DAOHostInfo) AvailableRAM(host *host.Host, result chan *hostitem, done <-chan bool) {
	rss := []*dao.RunningService{}
	if err := hi.dao.GetRunningServicesForHost(host.ID, &rss); err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", host.ID, err)
		return // this host won't be scheduled
	}

	var cr uint64

	for i := range rss {
		s := service.Service{}
		if err := hi.dao.GetService(rss[i].ServiceID, &s); err != nil {
			glog.Errorf("cannot retrieve service information for running service (%v)", err)
			return // this host won't be scheduled
		}

		cr += s.RAMCommitment
	}

	result <- &hostitem{host, host.Memory - cr, -1}
}
