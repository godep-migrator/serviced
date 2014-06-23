package service

import (
	"fmt"
	"testing"

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
}

func TestServiceListener_listenService(t *testing.T) {
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
	listener := NewServiceListener(conn, handler)
	spath := servicepath(svc.Id)
	hpath := hostpath(handler.Hosts[0].ID)

	// Start 5 instances and verify
	svc.Instances = 5
	listener.syncServiceInstances(svc, []string{})
	instances, err := conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != svc.Instances {
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	usedinstanceID := make(map[int]interface{})
	for i, id := range instances {
		var state servicestate.ServiceState
		spath := servicepath(svc.Id)
		if err := conn.Get(spath, &state); err != nil {
			t.Fatalf("Error while looking up %s: %s", spath, err)
		} else if _, ok := usedInstanceID[state.InstanceID]; ok {
			t.Errorf("DUPLICATE: found 2 instances with the same id: %d", state.InstanceID)
		}
		usedInstanceID[state.InstanceID] = nil

		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s: %s", hpath, err)
		} else if hs.DesiredState == service.SVCStop {
			t.Errorf("Found stopped service at %s", hpath)
		}
	}

	// Start 3 instances and verify
	svc.Instances = 8
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != svc.Instances {
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	usedinstanceID := make(map[int]interface{})
	for _, id := range instances {
		var state servicestate.ServiceState
		spath := servicepath(svc.Id)
		if err := conn.Get(spath, &state); err != nil {
			t.Fatalf("Error while looking up %s: %s", spath, err)
		} else if _, ok := usedInstanceID[state.InstanceID]; ok {
			t.Errorf("DUPLICATE: found 2 instances with the same id: %d", state.InstanceID)
		}
		usedInstanceID[state.InstanceID] = nil

		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s: %s", hpath, err)
		} else if hs.DesiredState == service.SVCStop {
			t.Errorf("Found stopped service at %s", hpath)
		}
	}

	// Stop 4 instances
	svc.Instances = 4
	listener.syncServiceInstances(svc, instances)
	instances, err = conn.Children(spath)
	if err != nil {
		t.Fatalf("Error while looking up %s: %s", spath, err)
	} else if count := len(instances); count != 8 { // Services are scheduled to be stopped, but haven't yet
		t.Errorf("MISMATCH: expected %d instances; actual %d", svc.Instances, count)
	}

	running := 0
	for _, id := range instances {
		var hs HostState
		hpath := hostpath(handler.Hosts[0].ID)
		if err := conn.Get(hpath, &hs); err != nil {
			t.Fatalf("Error while looking up %s", hpath, err)
		} else if hs.DesiredState == service.SVCRun {
			running++
		}
	}
	if svc.Instances == 

	// Stop 0 instances

	// Remove 2 stopped instances
	// Start 1 instance

}

func TestServiceListener_ServicesOnHost(t *testing.T) {
}

func TestUpdateService(t *testing.T) {
}

func TestRemoveService(t *testing.T) {
}