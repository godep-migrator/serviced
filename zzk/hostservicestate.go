package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/servicestate"
)

// HostServiceState bisects a ServiceState while setting the desired state
type HostServiceState struct {
	HostID         string
	ServiceID      string
	ServiceStateID string
	DesiredState   int
}

// NewHostServiceState initializes a new HostServiceState object
func NewHostServiceState(state *servicestate.ServiceState) *HostServiceState {
	return &HostServiceState{
		HostID:         state.HostId,
		ServiceID:      state.ServiceId,
		ServiceStateID: state.Id,
		DesiredState:   dao.SVC_RUN,
	}
}

func newHostServiceStateMessage(hss *HostServiceState, hostID, ssID string) *message {
	return newMessage(hss, zkHost, hostID, ssID)
}

// LoadHostServiceStateW returns a watch event for a HostServiceState node
func (z *Zookeeper) LoadHostServiceStateW(hss *HostServiceState, hostID, ssID string) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadHostServiceStateW(conn, hss, hostID, ssID)
	})
}

// LoadHostServiceStateW returns a watch event for a HostServiceState node
func LoadHostServiceStateW(conn client.Connection, hss *HostServiceState, hostID, ssID string) (<-chan client.Event, error) {
	msg := newHostServiceStateMessage(hss, hostID, ssID)
	return getW(conn, msg)
}

// LoadHostServiceState loads a particular HostServiceState
func (z *Zookeeper) LoadHostServiceState(hss *HostServiceState, ssID, hostID string) error {
	return z.call(func(conn client.Connection) error {
		return LoadHostServiceState(conn, hss, ssID, hostID)
	})
}

// LoadHostServiceState loads a particular HostServiceState
func LoadHostServiceState(conn client.Connection, hss *HostServiceState, ssID, hostID string) error {
	msg := newHostServiceStateMessage(hss, hostID, ssID)
	return get(conn, msg)
}

// AddHostServiceState creates a new HostServiceState node
func (z *Zookeeper) AddHostServiceState(hss *HostServiceState) error {
	return z.call(func(conn client.Connection) error {
		return AddHostServiceState(conn, hss)
	})
}

// AddHostServiceState creates a new HostServiceState node
func AddHostServiceState(conn client.Connection, hss *HostServiceState) error {
	msg := newHostServiceStateMessage(hss, hss.HostID, hss.ServiceStateID)
	return add(conn, msg)
}

// UpdateHostServiceState updates an existing host service state
func (z *Zookeeper) UpdateHostServiceState(hss *HostServiceState) error {
	return z.call(func(conn client.Connection) error {
		return UpdateHostServiceState(conn, hss)
	})
}

// UpdateHostServiceState updates an existing host service state
func UpdateHostServiceState(conn client.Connection, hss *HostServiceState) error {
	msg := newHostServiceStateMessage(hss, hss.HostID, hss.ServiceStateID)
	return update(conn, msg)
}

// LoadAndUpdateHostServiceState mutates a HostServiceState object with provided function
func (z *Zookeeper) LoadAndUpdateHostServiceState(hostID, ssID string, mutate func(*HostServiceState)) error {
	return z.call(func(conn client.Connection) error {
		return LoadAndUpdateHostServiceState(conn, hostID, ssID, mutate)
	})
}

// LoadAndUpdateHSS mutates a HostServiceState object with provided function
func LoadAndUpdateHostServiceState(conn client.Connection, hostID, ssID string, mutate func(*HostServiceState)) error {
	var hss HostServiceState
	if err := LoadHostServiceState(conn, &hss, hostID, ssID); err != nil {
		return err
	}
	mutate(&hss)
	return UpdateHostServiceState(conn, &hss)
}

// TerminateHostService terminates a ServiceState on a host
func (z *Zookeeper) TerminateHostService(hostID, ssID string) error {
	return z.call(func(conn client.Connection) error {
		return TerminateHostService(conn, hostID, ssID)
	})
}

// TerminateHostService terminates a ServiceState on a host
func TerminateHostService(conn client.Connection, hostID, ssID string) error {
	return LoadAndUpdateHostServiceState(conn, hostID, ssID, func(hss *HostServiceState) {
		(*hss).DesiredState = dao.SVC_STOP
	})
}