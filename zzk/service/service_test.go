package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/host"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

type TestServiceHandler struct {
	Hosts []*host.Host
	Index int
	Err   error
}

func (handler *TestServiceHandler) FindHostsInPool(poolID string) ([]*host.Host, error) {
	if handler.Err != nil {
		return nil, handler.Err
	}
	return handler.Hosts, nil
}

func (handler *TestServiceHandler) SelectHost(service *service.Service, hosts []*host.Host, policy host.HostPolicy) (*host.Host, error) {
	if handler.Err != nil {
		return nil, handler.Err
	}
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("no hosts found")
	}
	return hosts[handler.Index], nil
}

func TestServiceListener_Listen(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := &TestServiceHandler{
		Hosts: make([]*host.Host, 0),
		Index: 0,
	}
	handler.Hosts = append(handler.Hosts, &host.Host{
		ID:     "test-host-1",
		IPAddr: "test-host-1-ip",
	})

	t.Log("Start and stop listener with no services")
	shutdown := make(chan interface{})
	done := make(chan interface{})
	listener := NewServiceListener(conn, handler)
	go func() {
		listener.Listen(shutdown)
		close(done)
	}()

	<-time.After(2 * time.Second)
	t.Log("shutting down listener with no services")
	close(shutdown)
	<-done

	t.Log("Start and stop listener with multiple services")
	shutdown = make(chan interface{})
	done = make(chan interface{})
	go func() {
		listener.Listen(shutdown)
		close(done)
	}()

	svcs := []*service.Service{
		{
			Id:           "test-service-1",
			Endpoints:    make([]service.ServiceEndpoint, 1),
			DesiredState: service.SVCRun,
			Instances:    3,
		}, {
			Id:           "test-service-2",
			Endpoints:    make([]service.ServiceEndpoint, 1),
			DesiredState: service.SVCRun,
			Instances:    2,
		},
	}

	for _, s := range svcs {
		if err := conn.Create(servicepath(s.Id), s); err != nil {
			t.Fatalf("Could not create service %s: %s", s.Id, err)
		}
	}

	// wait for instances to start
	for {
		if rss, err := LoadRunningServices(conn); err != nil {
			t.Fatalf("Could not load running services: %s", err)
		} else if count := len(rss); count < 5 {
			<-time.After(time.Second)
		} else {
			break
		}
	}

	// shutdown
	t.Log("services started, now shutting down")
	close(shutdown)
	<-done

}

func TestServiceListener_listenService(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := &TestServiceHandler{
		Hosts: make([]*host.Host, 0),
		Index: 0,
	}
	handler.Hosts = append(handler.Hosts, &host.Host{
		ID:     "test-host-1",
		IPAddr: "test-host-1-ip",
	})

	// Add 1 service for 1 host
	svc := &service.Service{
		Id:           "test-service-1",
		Endpoints:    make([]service.ServiceEndpoint, 1),
		DesiredState: service.SVCStop,
	}
	spath := servicepath(svc.Id)
	if err := conn.Create(spath, svc); err != nil {
		t.Fatalf("Could not create service %s at %s: %s", svc.Id, spath, err)
	}

	var (
		shutdown = make(chan interface{})
		done     = make(chan string)
	)
	listener := NewServiceListener(conn, handler)
	go listener.listenService(shutdown, done, svc.Id)

	// Wait 3 seconds and shutdown
	<-time.After(3 * time.Second)
	t.Log("Signaling shutdown for service listener")
	close(shutdown)
	if serviceId := <-done; serviceId != svc.Id {
		t.Errorf("MISMATCH: invalid service id %s; expected: %s", serviceId, svc.Id)
	}

	// Start listener with 2 instances and stop service
	shutdown = make(chan interface{})
	done = make(chan string)
	listener = NewServiceListener(conn, handler)
	go listener.listenService(shutdown, done, svc.Id)

	getInstances := func() (count int) {
		var (
			instances []string
			err       error
		)

		for {
			instances, err = conn.Children(spath)
			if err != nil {
				t.Fatalf("Could not look up service instances for %s: %s", svc.Id, err)
			}
			if count := len(instances); count < svc.Instances {
				<-time.After(time.Second)
				continue
			} else {
				break
			}
		}

		for _, ssID := range instances {
			hpath := hostpath(handler.Hosts[0].ID, ssID)
			var hs HostState
			if err := conn.Get(hpath, &hs); err != nil {
				t.Fatalf("Could not look up instance %s: %s", ssID, err)
			}
			if hs.DesiredState == service.SVCRun {
				count++
			}
		}

		return count
	}

	t.Log("Starting service with 2 instances")
	svc.Instances = 2
	svc.DesiredState = service.SVCRun
	if err := conn.Set(spath, svc); err != nil {
		t.Fatalf("Could not update service %s at %s: %s", svc.Id, spath, err)
	}

	if count := getInstances(); count != svc.Instances {
		t.Errorf("Expected %d started instances; actual: %d", svc.Instances, count)
	}

	// Stop service
	t.Log("Stopping service")
	svc.DesiredState = service.SVCStop
	if err := conn.Set(spath, svc); err != nil {
		t.Fatalf("Could not update service %s at %s: %s", svc.Id, spath, err)
	}

	for {
		if count := getInstances(); count > 0 {
			t.Logf("Waiting for %d instances to stop", count)
			<-time.After(time.Second)
		} else {
			break
		}
	}

	// Remove the service
	t.Log("Removing service")
	if err := conn.Delete(spath); err != nil {
		t.Fatalf("Could not remove service %s at %s", svc.Id, spath, err)
	}
	if serviceId := <-done; serviceId != svc.Id {
		t.Errorf("MISMATCH: invalid service id %s; expected: %s", serviceId, svc.Id)
	}
}

func TestServiceListener_startServiceInstances(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := &TestServiceHandler{
		Hosts: make([]*host.Host, 0),
		Index: 0,
	}
	handler.Hosts = append(handler.Hosts, &host.Host{
		ID:     "test-host-1",
		IPAddr: "test-host-1-ip",
	})

	// Add 1 instance for 1 host
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}
	listener := NewServiceListener(conn, handler)
	listener.startServiceInstances(svc, handler.Hosts, []int{1})

	// Look up service instance
	var instance servicestate.ServiceState
	children, err := conn.Children(servicepath(svc.Id))
	if err != nil {
		t.Fatalf("Error while looking up service instances: %s", err)
	}
	if len(children) != 1 {
		t.Fatalf("Wrong number of instances found in path: %s", servicepath(svc.Id))
	}

	spath := servicepath(svc.Id, children[0])
	if err := conn.Get(spath, &instance); err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	}

	// Look up host state
	var hs HostState
	hpath := hostpath(handler.Hosts[0].ID, instance.Id)
	if err := conn.Get(hpath, &hs); err != nil {
		t.Fatalf("Error while looking up %s: %s", hpath, err)
	}

	// Check values
	if instance.Id != children[0] {
		t.Errorf("MISMATCH: instance id (%s) != node id (%s): %s", instance.Id, children[0], spath)
	}
	if instance.ServiceID != svc.Id {
		t.Errorf("MISMATCH: service ids do not match (%s != %s): %s", instance.ServiceID, svc.Id, spath)
	}
	if instance.HostID != handler.Hosts[0].ID {
		t.Errorf("MISMATCH: host ids do not match (%s != %s): %s", instance.HostID, handler.Hosts[0].ID, spath)
	}
	if instance.HostIP != handler.Hosts[0].IPAddr {
		t.Errorf("MISMATCH: host ips do not match (%s != %s): %s", instance.HostIP, handler.Hosts[0].IPAddr, spath)
	}
	if len(instance.Endpoints) != len(svc.Endpoints) {
		t.Errorf("MISMATCH: wrong number of endpoints (%d != %d): %s", len(instance.Endpoints), len(svc.Endpoints), spath)
	}

	if hs.ID != instance.Id {
		t.Errorf("MISMATCH: host state id (%s) != node id (%s): %s", hs.ID, instance.Id, hpath)
	}
	if hs.HostID != handler.Hosts[0].ID {
		t.Errorf("MISMATCH: host ids do not match (%s != %s): %s", hs.HostID, handler.Hosts[0].ID, hpath)
	}
	if hs.ServiceID != svc.Id {
		t.Errorf("MISMATCH: service ids do not match (%s != %s): %s", hs.ServiceID, svc.Id, hpath)
	}
	if hs.DesiredState != service.SVCRun {
		t.Errorf("MISMATCH: incorrect service state (%d != %d): %s", hs.DesiredState, service.SVCRun, hpath)
	}
}

func TestServiceListener_stopServiceInstances(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := &TestServiceHandler{
		Hosts: make([]*host.Host, 0),
		Index: 0,
	}
	handler.Hosts = append(handler.Hosts, &host.Host{
		ID:     "test-host-1",
		IPAddr: "test-host-1-ip",
	})

	// Add 2 instances for 1 host
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}
	listener := NewServiceListener(conn, handler)
	listener.startServiceInstances(svc, handler.Hosts, []int{1, 2})

	// Verify the number of instances
	spath := servicepath(svc.Id)
	instances, err := conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != 2 {
		t.Fatalf("MISMATCH: expected 2 children; found %d", count)
	}

	hpath := hostpath(handler.Hosts[0].ID)
	hoststates, err := conn.Children(hpath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", hpath, err)
	} else if count := len(hoststates); count != 2 {
		t.Fatalf("MISMATCH: expected 2 children; found %d", count)
	}

	// Stop 1 instance
	listener.stopServiceInstances(svc, []string{hoststates[0]})

	// Verify the state of the instances
	var hs HostState
	hpath = hostpath(handler.Hosts[0].ID, hoststates[0])
	if err := conn.Get(hpath, &hs); err != nil {
		t.Fatalf("Error while looking up %s: %s", hpath, err)
	} else if hs.DesiredState != service.SVCStop {
		t.Errorf("MISMATCH: expected service stopped (%d); actual (%d): %s", service.SVCStop, hs.DesiredState, hpath)
	}

	hpath = hostpath(handler.Hosts[0].ID, hoststates[1])
	if err := conn.Get(hpath, &hs); err != nil {
		t.Fatalf("Error while looking up %s: %s", hpath, err)
	} else if hs.DesiredState != service.SVCRun {
		t.Errorf("MISMATCH: expected service started (%d); actual (%d): %s", service.SVCRun, hs.DesiredState, hpath)
	}
}

func TestServiceListener_syncServiceInstances(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := &TestServiceHandler{
		Hosts: make([]*host.Host, 0),
		Index: 0,
	}
	handler.Hosts = append(handler.Hosts, &host.Host{
		ID:     "test-host-1",
		IPAddr: "test-host-1-ip",
	})
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}
	spath := servicepath(svc.Id)
	if err := conn.Create(spath, svc); err != nil {
		t.Fatalf("Error while creating node %s: %s", spath, err)
	}
	listener := NewServiceListener(conn, handler)

	instances, err := conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	}
	if len(instances) > 0 {
		t.Fatal("Expected 0 instances: ", instances)
	}

	// Start 5 instances and verify
	t.Log("Starting 5 instances")
	svc.Instances = 5
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != svc.Instances {
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	usedInstanceID := make(map[int]*servicestate.ServiceState)
	for _, id := range instances {
		var state servicestate.ServiceState
		spath := servicepath(svc.Id, id)
		if err := conn.Get(spath, &state); err != nil {
			t.Fatalf("Error while looking up %s: %s", spath, err)
		} else if ss, ok := usedInstanceID[state.InstanceID]; ok {
			t.Errorf("DUPLICATE: found 2 instances with the same id: [%v] [%v]", ss, state)
		}
		usedInstanceID[state.InstanceID] = &state

		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID, id)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s: %s", hpath, err)
		} else if hs.DesiredState == service.SVCStop {
			t.Errorf("Found stopped service at %s", hpath)
		}
	}

	// Start 3 instances and verify
	t.Log("Adding 3 more instances")
	svc.Instances = 8
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != svc.Instances {
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	usedInstanceID = make(map[int]*servicestate.ServiceState)
	for _, id := range instances {
		var state servicestate.ServiceState
		spath := servicepath(svc.Id, id)
		if err := conn.Get(spath, &state); err != nil {
			t.Fatalf("Error while looking up %s: %s", spath, err)
		} else if ss, ok := usedInstanceID[state.InstanceID]; ok {
			t.Errorf("DUPLICATE: found 2 instances with the same id: [%v] [%v]", ss, state)
		}
		usedInstanceID[state.InstanceID] = &state

		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID, id)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s: %s", hpath, err)
		} else if hs.DesiredState == service.SVCStop {
			t.Errorf("Found stopped service at %s", hpath)
		}
	}

	// Stop 4 instances
	t.Log("Stopping 4 instances")
	svc.Instances = 4
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != 8 { // Services are scheduled to be stopped, but haven't yet
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	var stopped []*HostState
	for _, id := range instances {
		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID, id)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s", hpath, err)
		} else if hs.DesiredState == service.SVCStop {
			stopped = append(stopped, &hs)
		}
	}
	if running := len(instances) - len(stopped); svc.Instances != running {
		t.Errorf("MISMATCH: expected %d running instances; actual %d", svc.Instances, running)
	}

	// Remove 2 stopped instances
	t.Log("Removing 2 stopped instances")
	for i := 0; i < 2; i++ {
		hs := stopped[i]
		hpath, spath := hostpath(hs.HostID, hs.ID), servicepath(hs.ServiceID, hs.ID)
		if err := conn.Delete(hpath); err != nil {
			t.Fatalf("Error while deleting %s: %s", hpath, err)
		} else if err := conn.Delete(spath); err != nil {
			t.Fatalf("Error while deleting %s: %s", spath, err)
		}
	}
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	}

	// Start 1 instance
	t.Log("Adding 1 more instance")
	svc.Instances = 5
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count < svc.Instances {
		t.Errorf("MISMATCH: expected AT LEAST %d running instances; actual %d", svc.Instances, count)
	}
}

func TestServiceListener_ServicesOnHost(t *testing.T) {
}

func TestUpdateService(t *testing.T) {
}

func TestRemoveService(t *testing.T) {
}