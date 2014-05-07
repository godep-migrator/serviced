package zzk

import (
	"path"

	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/servicestate"
)

type HostServiceState struct {
	HostID         string
	ServiceID      string
	ServiceStateID string
	DesiredState   int
}

func NewHostServiceState(state *servicestate.ServiceState) *HostServiceState {
	return &HostServiceState{
		HostID:         state.HostId,
		ServiceID:      state.ServiceId,
		ServiceStateID: state.Id,
		DesiredState:   dao.SVC_RUN,
	}
}

func NewHostServiceStateMessage(serviceStateID string, hostID string, hss *HostServiceState) Message {
	msg := NewMessage(serviceStateID, hss)
	msg.home = path.Join(zkHost, hostID)
	return msg
}

func (z *Zookeeper) LoadHostServiceStateW(hss *HostServiceState, serviceStateID string, hostID string) (<-chan client.Event, error) {
	msg := NewHostServiceStateMessage(serviceStateID, hostID, hss)
	return z.getW(&msg)
}

func (z *Zookeeper) LoadHostServiceState(hss *HostServiceState, serviceStateID string, hostID string) error {
	msg := NewHostServiceStateMessage(serviceStateID, hostID, hss)
	return z.call(&msg, get)
}

func (z *Zookeeper) UpdateHostServiceState(hss *HostServiceState) error {
	msg := NewHostServiceStateMessage(hss.ServiceStateID, hss.HostID, hss)
	return z.call(&msg, update)
}

func (z *Zookeeper) LoadAndUpdateHSS(serviceStateID string, hostID string, mutate func(*HostServiceState)) error {
	var hss HostServiceState
	if err := z.LoadHostServiceState(&hss, serviceStateID, hostID); err != nil {
		return err
	}
	mutate(&hss)
	return z.UpdateHostServiceState(&hss)
}

func (z *Zookeeper) TerminateHostService(serviceStateID string, hostID string) error {
	return z.LoadAndUpdateHSS(serviceStateID, hostID, func(hss *HostServiceState) {
		(*hss).DesiredState = dao.SVC_STOP
	})
}

func (z *Zookeeper) AddHostServiceState(hss *HostServiceState) error {
	msg := NewHostServiceStateMessage(hss.ServiceStateID, hss.HostID, hss)
	return z.call(&msg, add)
}