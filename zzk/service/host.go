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
	AttachService(chan<- interface{}, *service.Service, *servicestate.ServiceState) error
	StartService(chan<- interface{}, *service.Service, *servicestate.ServiceState) error
	StopService(*servicestate.ServiceState) error
	CheckInstance(*servicestate.ServiceState) error
}

type HostListener struct {
	hostID   string
	shutdown <-chan interface{}
	conn     client.Connection
	handler  HostHandler
}

func NewHostListener(shutdown <-chan interface{}, conn client.Connection, handler HostHandler, hostID string) *HostListener {
	return &HostListener{
		shutdown: shutdown,
		conn:     conn,
		handler:  handler,
		hostID:   hostID,
	}
}

func (l *HostListener) Listen() {
	var (
		shutdown   = make(chan interface{})
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

	defer func() {
		glog.Infof("Agent receieved interrupt")
		close(shutdown)
		for len(processing) > 0 {
			delete(processing, <-done)
		}
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

	var (
		attached bool
		procDone <-chan interface{}
	)

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
			var err error

			if state.Started.UnixNano() <= state.Terminated.UnixNano() {
				procDone, err = l.startInstance(&svc, &state)
			} else if !attached {
				procDone, err = l.attachInstance(&svc, &state)
			}
			if err != nil {
				glog.Errorf("Error trying to start or attach to service instance %s: %s", state.Id, err)
				l.stopInstance(&state)
				return
			}
			attached = true
		case service.SVCStop:
			if state.Started.UnixNano() >= state.Terminated.UnixNano() {
				if attached {
					l.detachInstance(procDone, &state)
				} else {
					l.stopInstance(&state)
				}
				return
			}
		default:
			glog.V(2).Infof("Unhandled service %s (%s)", svc.Name, svc.Id)
		}

		select {
		case <-procDone:
			glog.V(2).Info("Process ended for instance ", hs.ID)
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

func (l *HostListener) pingInstance(done <-chan interface{}, interval time.Duration, state *servicestate.ServiceState) {
	statepath := servicepath(state.ServiceID, state.Id)
	wait := time.After(time.Second)
	for {
		select {
		case <-wait:
			if err := l.handler.CheckInstance(state); err != nil {
				glog.V(2).Infof("Could not look up instance %s: %s", state.Id, err)
			} else if l.conn.Set(statepath, state); err != nil {
				glog.V(2).Infof("Could not update instance %s: %s", state.Id, err)
			}
			wait = time.After(interval * time.Second)
		case <-done:
			state.Terminated = time.Now()
			if err := l.conn.Set(statepath, state); err != nil {
				glog.V(2).Infof("Could not update instance %s as stopped: %s", state.Id, err)
			}
			return
		}
	}
}

func (l *HostListener) startInstance(svc *service.Service, state *servicestate.ServiceState) (<-chan interface{}, error) {
	done := make(chan interface{})
	if err := l.handler.StartService(done, svc, state); err != nil {
		return nil, err
	}
	go l.pingInstance(done, 5, state)
	return done, nil
}

func (l *HostListener) attachInstance(svc *service.Service, state *servicestate.ServiceState) (<-chan interface{}, error) {
	done := make(chan interface{})
	if err := l.handler.AttachService(done, svc, state); err != nil {
		return nil, err
	}
	go l.pingInstance(done, 5, state)
	return done, nil
}

func (l *HostListener) removeInstance(state *servicestate.ServiceState) error {
	if err := l.conn.Delete(hostpath(state.HostID, state.Id)); err != nil {
		return err
	}
	if err := l.conn.Delete(servicepath(state.ServiceID, state.Id)); err != nil {
		return err
	}
	return nil
}

func (l *HostListener) stopInstance(state *servicestate.ServiceState) error {
	if err := l.handler.StopService(state); err != nil {
		return err
	}
	return l.removeInstance(state)
}

func (l *HostListener) detachInstance(done <-chan interface{}, state *servicestate.ServiceState) error {
	if err := l.handler.StopService(state); err != nil {
		return err
	}
	<-done
	return l.removeInstance(state)
}