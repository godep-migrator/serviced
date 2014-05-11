package zzk

import (
	"time"

	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/servicestate"
)

func newServiceStateMessage(state *servicestate.ServiceState, serviceID, ssID string) *message {
	return newMessage(state, zkService, serviceID, ssID)
}

// LoadServiceStatesW watches for service state changes for a particular serviceID
func (z *Zookeeper) LoadServiceStatesW(states *[]*servicestate.ServiceState, serviceID string) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadServiceStatesW(conn, states, serviceID)
	})
}

// LoadServiceStatesW watches for service state changes for a particular serviceID
func LoadServiceStatesW(conn client.Connection, states *[]*servicestate.ServiceState, serviceID string) (<-chan client.Event, error) {
	if err := LoadServiceStates(conn, states, serviceID); err != nil {
		return nil, err
	}
	msg := newServiceMessage(nil, serviceID)
	return childrenW(conn, msg.path)
}

// LoadServiceState loads a service state given its ID and serviceID
func (z *Zookeeper) LoadServiceState(state *servicestate.ServiceState, serviceID, ssID string) error {
	return z.call(func(conn client.Connection) error {
		return LoadServiceState(conn, state, serviceID, ssID)
	})
}

// LoadServiceState loads a service state given its ID and serviceID
func LoadServiceState(conn client.Connection, state *servicestate.ServiceState, serviceID, ssID string) error {
	msg := newServiceStateMessage(state, serviceID, ssID)
	return get(conn, msg)
}

// LoadServiceStates loads service states by the service ID
func (z *Zookeeper) LoadServiceStates(states *[]*servicestate.ServiceState, serviceIDs ...string) error {
	return z.call(func(conn client.Connection) error {
		return LoadServiceStates(conn, states, serviceIDs...)
	})
}

func LoadServiceStates(conn client.Connection, states *[]*servicestate.ServiceState, serviceIDs ...string) error {
	for _, serviceID := range serviceIDs {
		msg := newServiceMessage(nil, serviceID)
		err := children(conn, msg.path, func(ssID string) error {
			var state servicestate.ServiceState
			if err := LoadServiceState(conn, &state, serviceID, ssID); err != nil {
				return err
			}
			*states = append(*states, &state)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// AddServiceState creates a new service state
func (z *Zookeeper) AddServiceState(state *servicestate.ServiceState) error {
	return z.call(func(conn client.Connection) error {
		return AddServiceState(conn, state)
	})
}

// AddServiceState creates a new service state
func AddServiceState(conn client.Connection, state *servicestate.ServiceState) error {
	msg := newServiceStateMessage(state, state.ServiceId, state.Id)
	if err := add(conn, msg); err != nil {
		return err
	}
	hss := NewHostServiceState(state)
	return AddHostServiceState(conn, hss)
}

// UpdateServiceState updates an existing service state
func (z *Zookeeper) UpdateServiceState(state *servicestate.ServiceState) error {
	return z.call(func(conn client.Connection) error {
		return UpdateServiceState(conn, state)
	})
}

// UpdateServiceState updates an existing service state
func UpdateServiceState(conn client.Connection, state *servicestate.ServiceState) error {
	msg := newServiceStateMessage(state, state.ServiceId, state.Id)
	return update(conn, msg)
}

// RemoveServiceState removes an existing service state
func (z *Zookeeper) RemoveServiceState(hostID, serviceID, ssID string) error {
	return z.call(func(conn client.Connection) error {
		return RemoveServiceState(conn, hostID, serviceID, ssID)
	})
}

// RemoveServiceState removes an existing service state
func RemoveServiceState(conn client.Connection, hostID, serviceID, ssID string) error {
	if err := RemoveHostServiceState(conn, hostID, ssID); err != nil {
		return err
	}
	msg := newServiceStateMessage(nil, serviceID, ssID)
	return remove(conn, msg)
}

// LoadAndUpdateServiceState loads a service state and mutates it accordingly
func (z *Zookeeper) LoadAndUpdateServiceState(serviceID, ssID string, mutate func(*servicestate.ServiceState)) error {
	return z.call(func(conn client.Connection) error {
		return LoadAndUpdateServiceState(conn, serviceID, ssID, mutate)
	})
}

// LoadAndUpdateServiceState loads a service state and mutates it accordingly
func LoadAndUpdateServiceState(conn client.Connection, serviceID, ssID string, mutate func(*servicestate.ServiceState)) error {
	var state servicestate.ServiceState
	if err := LoadServiceState(conn, &state, serviceID, ssID); err != nil {
		return err
	}
	mutate(&state)
	return UpdateServiceState(conn, &state)
}

// ResetServiceState resets a service state
func (z *Zookeeper) ResetServiceState(serviceID, ssID string) error {
	return z.call(func(conn client.Connection) error {
		return ResetServiceState(conn, serviceID, ssID)
	})
}

// ResetServiceState resets a service state
func ResetServiceState(conn client.Connection, serviceID, ssID string) error {
	return LoadAndUpdateServiceState(conn, serviceID, ssID, func(state *servicestate.ServiceState) {
		(*state).Terminated = time.Now()
	})
}