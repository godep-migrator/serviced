package service

import (
	"path"
	"time"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
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

type HostHandler interface {
	Start(chan<- interface{}, *service.Service, *servicestate.ServiceState) error
	Stop(*servicestate.ServiceState) error
	Attach(chan<- interface{}, *servicestate.ServiceState) error
	Detach(*servicestate.ServiceState) error
}

type HostListener struct {
	hostID   string
	shutdown <-chan interface{}
	conn     client.Connection
	handler  HostHandler
}

func (l *HostListener) Listen() {
	shutdown := make(chan interface{})
	defer func() {
		glog.Infof("Agent receieved interrupt")
		close(shutdown)
	}()

	path := hostpath(l.hostID)
	if exists, err := l.conn.Exists(path); err != nil {
		glog.Errorf("Unable to look up host path %s on zookeeper: %s", l.hostID, err)
		return
	} else if exists {
		// pass
	} else if err := l.conn.CreateDir(path); err != nil {
		glog.Errorf("Unable to create host path %s on zookeeper: %s", l.hostID, err)
		return
	}

	var (
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

	for {
		stateIDs, event, err := l.conn.ChildrenW(path)
		if err != nil {
			glog.Errorf("Could not watch for states on host %s: %s", l.hostID, err)
			return
		}

		for _, ssID := range stateIDs {
			if _, ok := processing[ssID]; !ok {
				glog.V(1).Info("Spawning a listener for SSID ", ssID)
				processing[ssID] = nil
				go l.listenHostState(shutdown, done, ssID)
			}
		}

		select {
		case e := <-event:
			glog.V(2).Infof("Receieved event: %v", e)
		case ssID := <-done:
			glog.V(2).Info("Cleaning up SSID ", ssID)
			delete(processing, ssID)
		case <-l.shutdown:
			return
		}
	}
}

func (l *HostListener) listenHostState(shutdown <-chan interface{}, done chan<- string, ssID string) {
	defer func() {
		glog.V(2).Info("Shutting down listener for host instance ", ssID)
		done <- ssID
	}()

	for {
		var hs HostState
		event, err := l.conn.GetW(hostpath(l.hostID, ssID), &hs)
		if err != nil {
			glog.Errorf("Could not load host instance %s: %s", ssID, err)
			return
		}

		if hs.ServiceID == "" || hs.ID == "" {
			glog.Error("Invalid host state instance: ", hostpath(l.hostID, ssID))
			return
		}

		var state servicestate.ServiceState
		if err := l.conn.Get(servicepath(hs.ServiceID, hs.ID), &state); err != nil {
			glog.Error("Could find service instance: ", hs.ID)
			// Node doesn't exist or cannot be loaded.  Delete it.
			if err := l.conn.Delete(hostpath(l.hostID, ssID)); err != nil {
				glog.Warningf("Could not delete host instance %s: %s", ssID, err)
			}
			return
		}

		var svc service.Service
		if err := l.conn.Get(servicepath(hs.ServiceID), &svc); err != nil {
			glog.Error("Could not find service: ", hs.ServiceID)
			return
		}

		glog.V(2).Infof("Processing %s (%s); Desired State: %s", svc.Name, svc.Id, hs.DesiredState)
		switch hs.DesiredState {
		case service.SVCRun:
			if state.Started.UnixNano() <= state.Terminated.UnixNano() {
				l.startInstance(&svc, &state)
			} else {
				l.attachInstance(&state)
			}
		case service.SVCStop:
			if state.Started.UnixNano() >= state.Terminated.UnixNano() {
				if attached {
					l.detachInstance(&state)
				} else {

				}
			}
		default:
			// Unknown service state
		}

		select {
		case e := <-event:
			glog.V(3).Infof("Receieved event: %v", e)
			switch e.Type {
			case client.EventNodeDeleted:
				// node was deleted, so process was terminated
				return
			}
		case <-shutdown:
			glog.V(2).Infof("Host instance %s received signal to shutdown", hs.ID)
			// Stop service instance
			return
		}
	}
}

func (l *HostListener) startInstance(svc *service.Service, state *servicestate.ServiceState) {
	done := make(chan interface{})
	if err := l.handler.Start(done, svc, state); err != nil {
		glog.Errorf("Could not start service instance %s: %s", state.Id, err)
		return
	}
	state.Started = time.Now()
	if err := l.conn.Set(servicepath(state.ServiceID, state.Id), state); err != nil {
		glog.Errorf("Could update service instance %s as started: %s", state.Id, err)
	}
	go func() {
		<-done
		state.Terminated = time.Now()
		if err := l.conn.Set(servicepath(state.ServiceID, state.Id), state); err != nil {
			glog.Errorf("Could not update service instance %s as stopped: %s", state.Id, err)
		}
	}()
}

func (l *HostListener) stopInstance(state *servicestate.ServiceState) {
	if err := l.handler.Stop(state); err != nil {
		glog.Errorf("Could not stop service instance %s: %s", state.Id, err)
		return
	}
	if err := l.conn.Delete(hostpath(state.HostID, state.Id)); err != nil {
		glog.Errorf("Could not delete host instance %s: %s", state.Id, err)
		return
	}
	if err := l.conn.Delete(servicepath(state.ServiceID, state.Id)); err != nil {
		glog.Errorf("Could not delete service instance %s: %s", state.Id, err)
		return
	}
}

func (l *HostListener) attachInstance(hs *HostState) {
}

// Stop the service
// When done, delete node