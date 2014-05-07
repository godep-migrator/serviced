package zzk

import (
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

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

func (z *Zookeeper) LoadRunningServicesByHost(running *[]*dao.RunningService, hostIDs ...string) error {
	for _, hostID := range hostIDs {
		hostMessage := NewHostMessage(hostID, nil)
		states, err := z.children(&hostMessage)
		if err != nil {
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

func (z *Zookeeper) LoadRunningServices(running *[]*dao.RunningService) error {
	msg := NewMessage(zkService, nil)
	serviceIDs, err := z.children(&msg)
	if err != nil {
		return err
	}

	return z.LoadRunningServicesByService(running, serviceIDs...)
}