package service

import (
	"fmt"
	"testing"

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

func (handler *TestHostHandler) StopInstance(state *servicestate.ServiceState) error {
	if instanceC, ok := handler.processing[state.Id]; ok {
		delete(handler.processing, state.Id)
		delete(handler.states, state.Id)
		close(instanceC)
		return nil
	}

	return fmt.Errorf("instance %s not running", state.Id)
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
}

func TestHostListener_stopInstance(t *testing.T) {
}

func TestHostListener_detachInstance(t *testing.T) {
}