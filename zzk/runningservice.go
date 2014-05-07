package zzk

import (
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

const (
	zkHost = "/hosts"
)

// NewRunningService initializes a new RunningService by bisecting a service with a service state
func NewRunningService(service *service.Service, state *servicestate.ServiceState) *dao.RunningService {
	return &dao.RunningService{
		Id:              state.Id,
		ServiceId:       state.ServiceId,
		StartedAt:       state.Started,
		HostId:          state.HostId,
		DockerId:        state.DockerId,
		InstanceId:      state.InstanceId,
		Startup:         service.Startup,
		Name:            service.Name,
		Description:     service.Description,
		Instances:       service.Instances,
		PoolId:          service.PoolId,
		ImageId:         service.ImageId,
		DesiredState:    service.DesiredState,
		ParentServiceId: service.ParentServiceId,
	}
}

// LoadRunningService loads a running service node
func (z *Zookeeper) LoadRunningService(running *dao.RunningService, serviceStateID string, serviceID string) error {
	var service service.Service
	if err := z.LoadService(&service, serviceID); err != nil {
		return err
	}

	var state servicestate.ServiceState
	if err := z.LoadServiceState(&state, serviceStateID, serviceID); err != nil {
		return err
	}

	*running = *NewRunningService(&service, &state)
	return nil
}

// LoadRunningServicesByHost gets all of the running services via a hostID lookup
func (z *Zookeeper) LoadRunningServicesByHost(running *[]*dao.RunningService, hostIDs ...string) error {
	for _, hostID := range hostIDs {
		var states []string
		hostMessage := Message{
			home:    zkHost,
			id:      hostID,
			Payload: &states,
		}
		if err := z.call(&hostMessage, children); err != nil {
			return err
		}
		for _, ssid := range states {
			var hss HostServiceState
			if err := z.LoadHostServiceState(&hss, ssid, hostID); err != nil {
				return err
			}

			var service service.Service
			if err := z.LoadService(&service, hss.ServiceID); err != nil {
				return err
			}

			var state servicestate.ServiceState
			if err := z.LoadServiceState(&state, hss.ServiceStateID, hss.ServiceID); err != nil {
				return err
			}

			*running = append(*running, NewRunningService(&service, &state))
		}
	}

	return nil
}

// LoadRunningServicesByService gets all of the running services via a serviceID lookup
func (z *Zookeeper) LoadRunningServicesByService(running *[]*dao.RunningService, serviceIDs ...string) error {
	for _, serviceID := range serviceIDs {
		var service service.Service
		if err := z.LoadService(&service, serviceID); err != nil {
			return err
		}

		var states []*servicestate.ServiceState
		if err := z.LoadServiceStatesByService(&states, serviceID); err != nil {
			return err
		}

		for _, s := range states {
			*running = append(*running, NewRunningService(&service, s))
		}
	}
	return nil
}

// LoadRunningServices gets all of the running services
func (z *Zookeeper) LoadRunningServices(running *[]*dao.RunningService) error {
	var serviceIDs []string
	msg := Message{
		home:    zkService,
		Payload: &serviceIDs,
	}
	if err := z.call(&msg, children); err != nil {
		return err
	}
	return z.LoadRunningServicesByService(running, serviceIDs...)
}