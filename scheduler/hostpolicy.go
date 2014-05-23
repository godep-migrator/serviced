package scheduler

import (
	"errors"

	"github.com//zenoss/serviced/coordinator/client"
	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicedefinition"
)

// ServiceHostPolicy wraps a service and provides several policy
// implementations for choosing hosts on which to run instances of that
// service.
type ServiceHostPolicy struct {
	svc   *service.Service
	hinfo HostInfo
}

// ServiceHostPolicy returns a new ServiceHostPolicy.
func NewServiceHostPolicy(s *service.Service, cp dao.ControlPlane) *ServiceHostPolicy {
	return &ServiceHostPolicy{s, &DAOHostInfo{cp}}
}

func NewZKServiceHostPolicy(s *service.Service, conn client.Connection) *ServiceHostPolicy {
	return &ServiceHostPolicy{s, &ZKHostInfo{conn}}
}

func (sp *ServiceHostPolicy) SelectHost(hosts []*host.Host) (*host.Host, error) {
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

func (sp *ServiceHostPolicy) firstFreeHost(svc *service.Service, hosts []*host.Host) *host.Host {
hosts:
	for _, h := range hosts {
		rss := sp.hinfo.ServicesOnHost(h)
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
func (sp *ServiceHostPolicy) leastCommittedHost(hosts []*host.Host) (*host.Host, error) {
	var (
		prioritized []*host.Host
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
func (sp *ServiceHostPolicy) preferSeparateHosts(hosts []*host.Host) (*host.Host, error) {
	var (
		prioritized []*host.Host
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
func (sp *ServiceHostPolicy) requireSeparateHosts(hosts []*host.Host) (*host.Host, error) {
	var (
		prioritized []*host.Host
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

func (sp *ServiceHostPolicy) prioritizeByMemory(hosts []*host.Host) ([]*host.Host, error) {
	var wg sync.WaitGroup

	result := make([]*host.Host, 0)
	done := make(chan bool)
	defer close(done)

	hic := make(chan *hostitem)

	// fan-out available RAM computation for each host
	for _, h := range hosts {
		wg.Add(1)
		go func(host *host.Host) {
			sp.hinfo.AvailableRAM(host, hic, done)
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
