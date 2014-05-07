package zzk

import (
	"path"

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

// NewHostServiceStateMessage initializes a new message for HostServiceState nodes
func NewHostServiceStateMessage(serviceStateID string, hostID string, hss *HostServiceState) Message {
	msg := NewMessage(serviceStateID, hss)
	msg.home = path.Join(zkHost, hostID)
	return msg
}

// LoadHostServiceStateW returns a watch event for a HostServiceState node
func (z *Zookeeper) LoadHostServiceStateW(hss *HostServiceState, serviceStateID string, hostID string) (<-chan client.Event, error) {
	msg := NewHostServiceStateMessage(serviceStateID, hostID, hss)
	return z.getW(&msg, getW)
}

// LoadHostServiceState loads a particular HostServiceState
func (z *Zookeeper) LoadHostServiceState(hss *HostServiceState, serviceStateID string, hostID string) error {
	msg := NewHostServiceStateMessage(serviceStateID, hostID, hss)
	return z.call(&msg, get)
}

// UpdateHostServiceState updates an existing host service state
func (z *Zookeeper) UpdateHostServiceState(hss *HostServiceState) error {
	msg := NewHostServiceStateMessage(hss.ServiceStateID, hss.HostID, hss)
	return z.call(&msg, update)
}

// LoadAndUpdateHSS mutates a HostServiceState object with provided function
func (z *Zookeeper) LoadAndUpdateHSS(serviceStateID string, hostID string, mutate func(*HostServiceState)) error {
	var hss HostServiceState
	if err := z.LoadHostServiceState(&hss, serviceStateID, hostID); err != nil {
		return err
	}
	mutate(&hss)
	return z.UpdateHostServiceState(&hss)
}

// TerminateHostService terminates a ServiceState on a host
func (z *Zookeeper) TerminateHostService(serviceStateID string, hostID string) error {
	return z.LoadAndUpdateHSS(serviceStateID, hostID, func(hss *HostServiceState) {
		(*hss).DesiredState = dao.SVC_STOP
	})
}

// AddHostServiceState creates a new HostServiceState node
func (z *Zookeeper) AddHostServiceState(hss *HostServiceState) error {
	msg := NewHostServiceStateMessage(hss.ServiceStateID, hss.HostID, hss)
	return z.call(&msg, add)
}