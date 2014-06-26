package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

type TestHostHandler struct {
	processing map[string]chan<- interface{}
	states     map[string]*servicestate.ServiceState
}

func NewTestHostHandler() *TestHostHandler {
	return new(TestHostHandler).init()
}

func (handler *TestHostHandler) init() *TestHostHandler {
	*handler = TestHostHandler{
		processing: make(map[string]chan<- interface{}),
		states:     make(map[string]*servicestate.ServiceState),
	}
	return handler
}

func (handler *TestHostHandler) AttachService(done chan<- interface{}, svc *service.Service, state *servicestate.ServiceState) error {
	if instanceC, ok := handler.processing[state.Id]; ok {
		delete(handler.processing, state.Id)
		close(instanceC)
		handler.processing[state.Id] = done
		return nil
	}

	return fmt.Errorf("instance %s not running", state.Id)
}

func (handler *TestHostHandler) StartService(done chan<- interface{}, svc *service.Service, state *servicestate.ServiceState) error {
	if _, ok := handler.processing[state.Id]; !ok {
		handler.processing[state.Id] = done
		handler.states[state.Id] = state
		return nil
	}

	return fmt.Errorf("instance %s already started", state.Id)
}

func (handler *TestHostHandler) StopService(state *servicestate.ServiceState) error {
	if instanceC, ok := handler.processing[state.Id]; ok {
		delete(handler.processing, state.Id)
		delete(handler.states, state.Id)
		close(instanceC)
	}
	return nil
}

func (handler *TestHostHandler) CheckInstance(state *servicestate.ServiceState) error {
	if s, ok := handler.states[state.Id]; ok {
		*state = *s
		return nil
	}

	return fmt.Errorf("instance %s not found", state.Id)
}

func (handler *TestHostHandler) UpdateInstance(state *servicestate.ServiceState) error {
	if _, ok := handler.states[state.Id]; ok {
		handler.states[state.Id] = state
		return nil
	}

	return fmt.Errorf("instance %s not found", state.Id)
}

func TestHostListener_Listen(t *testing.T) {
}

func TestHostListener_listenHostState(t *testing.T) {
}

func TestHostListener_pingInstance(t *testing.T) {
}

func TestHostListener_startInstance(t *testing.T) {
}

func TestHostListener_attachInstance(t *testing.T) {
}

func TestHostListener_removeInstance(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := NewTestHostHandler()
	listener := NewHostListener(conn, handler, "test-host-1")

	// Create the instance
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}

	t.Log("Adding and deleting an instance from the connection")
	if state, err := servicestate.BuildFromService(svc, listener.hostID); err != nil {
		t.Fatalf("Could not generate instance from service %s", svc.Id)
	} else if err := addInstance(conn, state); err != nil {
		t.Fatalf("Could not add instance %s from service %s", state.Id, state.ServiceID)
	} else if err := listener.removeInstance(state); err != nil {
		t.Fatalf("Error while removing instance %s from service %s", state.Id, state.ServiceID)
	} else if exists, err := conn.Exists(servicepath(state.ServiceID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from service %s", state.Id, state.ServiceID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.ServiceID)
	} else if exists, err := conn.Exists(hostpath(state.HostID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from host %s", state.Id, state.HostID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.HostID)
	}
}

func TestHostListener_stopInstance(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := NewTestHostHandler()
	listener := NewHostListener(conn, handler, "test-host-1")

	// Create the instance
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}

	t.Log("Adding and stopping an instance from the connection")
	if state, err := servicestate.BuildFromService(svc, listener.hostID); err != nil {
		t.Fatalf("Could not generate instance from service %s", svc.Id)
	} else if err := addInstance(conn, state); err != nil {
		t.Fatalf("Could not add instance %s from service %s", state.Id, state.ServiceID)
	} else if err := listener.stopInstance(state); err != nil {
		t.Fatalf("Could not stop instance %s from service %s", state.Id, state.ServiceID)
	} else if exists, err := conn.Exists(servicepath(state.ServiceID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from service %s", state.Id, state.ServiceID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.ServiceID)
	} else if exists, err := conn.Exists(hostpath(state.HostID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from host %s", state.Id, state.HostID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.HostID)
	}
}

func TestHostListener_detachInstance(t *testing.T) {
	conn := client.NewTestConnection()
	defer conn.Close()
	handler := NewTestHostHandler()
	listener := NewHostListener(conn, handler, "test-host-1")

	// Create the instance
	svc := &service.Service{
		Id:        "test-service-1",
		Endpoints: make([]service.ServiceEndpoint, 1),
	}

	t.Log("Adding an instance from the connection and waiting to detach")
	state, err := servicestate.BuildFromService(svc, listener.hostID)
	if err != nil {
		t.Fatalf("Could not generate instance from service %s", svc.Id)
	} else if err := addInstance(conn, state); err != nil {
		t.Fatalf("Could not add instance %s from service %s", state.Id, state.ServiceID)
	}

	done := make(chan interface{})
	wait := make(chan interface{})
	go func() {
		defer close(wait)
		if err := listener.detachInstance(done, state); err != nil {
			t.Fatalf("Could not detach instance %s from service %s", state.Id, state.ServiceID)
		}
	}()

	<-time.After(time.Second)
	close(done)
	<-wait

	if exists, err := conn.Exists(servicepath(state.ServiceID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from service %s", state.Id, state.ServiceID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.ServiceID)
	} else if exists, err := conn.Exists(hostpath(state.HostID, state.Id)); err != nil {
		t.Fatalf("Error while checking the existance of %s from host %s", state.Id, state.HostID)
	} else if exists {
		t.Errorf("Failed to delete node %s from %s", state.Id, state.HostID)
	}
}