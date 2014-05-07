package zzk

import (
	"path"
	"time"

	"github.com/zenoss/serviced/domain/servicestate"
)

// NewServiceStateMessage initializes a new service state message
func NewServiceStateMessage(id string, serviceID string, state *servicestate.ServiceState) Message {
	msg := NewMessage(id, state)
	msg.home = path.Join(zkService, serviceID)
	return msg
}

// LoadServiceState loads a service state given its ID and serviceID
func (z *Zookeeper) LoadServiceState(state *servicestate.ServiceState, id string, serviceID string) error {
	msg := NewServiceStateMessage(id, serviceID, state)
	return z.call(&msg, get)
}

// LoadServiceStatesByService loads service states by the service ID
func (z *Zookeeper) LoadServiceStatesByService(states *[]*servicestate.ServiceState, serviceIDs ...string) error {
	for _, serviceID := range serviceIDs {
		// Get the child nodes of the service
		var ssids []string
		serviceMessage := Message{
			home:    zkService,
			id:      serviceID,
			Payload: &ssids,
		}
		if err := z.call(&serviceMessage, children); err != nil {
			return err
		}

		// Get the service states for each child of the service
		for _, ssid := range ssids {
			var state servicestate.ServiceState
			if err := z.LoadServiceState(&state, ssid, serviceID); err != nil {
				return err
			}
			*states = append(*states, &state)
		}
	}
	return nil
}

// AddServiceState creates a new service state
func (z *Zookeeper) AddServiceState(state *servicestate.ServiceState) error {
	msg := NewServiceStateMessage(state.Id, state.ServiceId, state)
	if err := z.call(&msg, add); err != nil {
		return err
	}
	return z.AddHostServiceState(NewHostServiceState(state))
}

// UpdateServiceState updates an existing service state
func (z *Zookeeper) UpdateServiceState(state *servicestate.ServiceState) error {
	msg := NewServiceStateMessage(state.Id, state.ServiceId, state)
	return z.call(&msg, update)
}

// LoadAndUpdateServiceState loads a service state and mutates it accordingly
func (z *Zookeeper) LoadAndUpdateServiceState(id string, serviceID string, mutate func(*servicestate.ServiceState)) error {
	var state servicestate.ServiceState
	if err := z.LoadServiceState(&state, id, serviceID); err != nil {
		return err
	}
	mutate(&state)
	return z.UpdateServiceState(&state)
}

// ResetServiceState resets a service state
func (z *Zookeeper) ResetServiceState(id string, serviceID string) error {
	return z.LoadAndUpdateServiceState(id, serviceID, func(state *servicestate.ServiceState) {
		(*state).Terminated = time.Now()
	})
}