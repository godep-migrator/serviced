package service

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

const (
	zkHost = "/hosts"
)

func hostpath(nodes ...string) string {
	p := append([]string{zkHost}, nodes...)
	return path.Join(p...)
}

type HostState struct {
	ID           string
	HostID       string
	ServiceID    string
	DesiredState int

	version interface{}
}

func NewHostState(state *servicestate.ServiceState) *HostState {
	return &HostState{
		HostID:       state.HostID,
		ServiceID:    state.ServiceID,
		ID:           state.Id,
		DesiredState: service.SVCRun,
	}
}

func (hs *HostState) Version() interface{}           { return hs.version }
func (hs *HostState) SetVersion(version interface{}) { hs.version = version }

type ZKHostInfo struct {
	conn client.Connection
}

func (hi *ZKHostInfo) AvailableRAM(h *host.Host, result chan<- *host.Item) {
	rss, err := LoadRunningServicesByHost(hi.conn, h.ID)
	if err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", h.ID, err)
		return // this host won't be scheduled
	}

	var totalRAM uint64
	for _, rs := range rss {
		totalRAM += rs.RAMCommitment
	}

	result <- &host.Item{h, h.Memory - totalRAM, -1}
}

func (hi *ZKHostInfo) ServicesOnHost(host *host.Host) []*dao.RunningService {
	rss, err := LoadRunningServicesByHost(hi.conn, host.ID)
	if err != nil {
		glog.Errorf("cannot retrieve running services for host: %s (%v)", host.ID, err)
	}
	return rss
}