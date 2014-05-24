package host

import (
	"container/heap"
	"errors"
	"sync"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicedefinition"
)

type HostInfo interface {
	ServicesOnHost(host *Host) ([]*dao.RunningService, error)
}

type HostPolicy interface {
	Select(hosts []*Host) (*Host, error)
}

type ServiceHostPolicy struct {
	svc   *service.Service
	hinfo HostInfo
}

func NewServiceHostPolicy(svc *service.Service, hinfo HostInfo) *ServiceHostPolicy {
	return &ServiceHostPolicy{svc, hinfo}
}

func (sp *ServiceHostPolicy) Select(hosts []*Host) (*Host, error) {
	switch sp.svc.HostPolicy {
	case servicedefinition.PreferSeparate:
		glog.V(2).Infof("Using PREFER_SEPARATE host policy")
		return sp.preferSeparateHosts(hosts)
	case servicedefinition.RequireSeparate:
		glog.V(2).Infof("Using REQUIRE_SEPARATE host policy")
		return sp.requireSeparateHosts(hosts)
	default:
		glog.V(2).Infof("Using LEAST_COMMITTED host policy")
		return sp.leastCommittedHost(hosts)
	}
}

func (sp *ServiceHostPolicy) AvailableRAM(host *Host, result chan<- *hostitem) {
	rss, err := sp.hinfo.ServicesOnHost(host)
	if err != nil {
		// this host will not be scheduled
		glog.Errorf("Cannot retrieve running services for host %s: %s", host.ID, err)
		return
	}

	var totalRAM uint64
	for _, rs := range rss {
		totalRAM += rs.RAMCommitment
	}
	result <- &hostitem{host, host.Memory - totalRAM, -1}
}

func (sp *ServiceHostPolicy) firstFreeHost(svc *service.Service, hosts []*Host) *Host {
hosts:
	for _, h := range hosts {
		rss, _ := sp.hinfo.ServicesOnHost(h)
		if rss == nil {
			rss = make([]*dao.RunningService, 0)
		}
		for _, rs := range rss {
			if rs.ServiceID == svc.Id {
				// This host already has an instance of this service. Move on.
				continue hosts
			}
		}
		return h
	}
	return nil
}

// leastCommittedHost chooses the host with the least RAM committed to running
// containers.
func (sp *ServiceHostPolicy) leastCommittedHost(hosts []*Host) (*Host, error) {
	var (
		prioritized []*Host
		err         error
	)
	if prioritized, err = sp.prioritizeByMemory(hosts); err != nil {
		return nil, err
	}
	return prioritized[0], nil
}

// preferSeparateHosts chooses the least committed host that isn't already
// running an instance of the service. If all hosts are running an instance of
// the service already, it returns the least committed host.
func (sp *ServiceHostPolicy) preferSeparateHosts(hosts []*Host) (*Host, error) {
	var (
		prioritized []*Host
		err         error
	)
	if prioritized, err = sp.prioritizeByMemory(hosts); err != nil {
		return nil, err
	}
	// First pass: find one that isn't running an instance of the service
	if h := sp.firstFreeHost(sp.svc, prioritized); h != nil {
		return h, nil
	}
	// Second pass: just find an available host
	for _, h := range prioritized {
		return h, nil
	}
	return nil, errors.New("Unable to find a host to schedule")
}

// requireSeparateHosts chooses the least committed host that isn't already
// running an instance of the service. If all hosts are running an instance of
// the service already, it returns an error.
func (sp *ServiceHostPolicy) requireSeparateHosts(hosts []*Host) (*Host, error) {
	var (
		prioritized []*Host
		err         error
	)
	if prioritized, err = sp.prioritizeByMemory(hosts); err != nil {
		return nil, err
	}
	// First pass: find one that isn't running an instance of the service
	if h := sp.firstFreeHost(sp.svc, prioritized); h != nil {
		return h, nil
	}
	// No second pass
	return nil, errors.New("Unable to find a host to schedule")
}

func (sp *ServiceHostPolicy) prioritizeByMemory(hosts []*Host) ([]*Host, error) {
	var wg sync.WaitGroup

	result := make([]*Host, 0)
	hic := make(chan *hostitem)

	// fan-out available RAM computation for each host
	for _, h := range hosts {
		wg.Add(1)
		go func(host *Host) {
			sp.AvailableRAM(host, hic)
			wg.Done()
		}(h)
	}

	// close the hostitem channel when all the calculation is finished
	go func() {
		wg.Wait()
		close(hic)
	}()

	pq := &PriorityQueue{}
	heap.Init(pq)

	// fan-in all the available RAM computations
	for hi := range hic {
		heap.Push(pq, hi)
	}

	if pq.Len() < 1 {
		return nil, errors.New("Unable to find a host to schedule")
	}

	for pq.Len() > 0 {
		result = append(result, heap.Pop(pq).(*hostitem).host)
	}
	return result, nil
}