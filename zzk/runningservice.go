package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
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
func (z *Zookeeper) LoadRunningService(rs *dao.RunningService, serviceID, ssID string) error {
	return z.call(func(conn client.Connection) error {
		return LoadRunningService(conn, rs, serviceID, ssID)
	})
}

func LoadRunningService(conn client.Connection, rs *dao.RunningService, serviceID string, ssID string) error {
	var service service.Service
	if err := LoadService(conn, &service, serviceID); err != nil {
		return err
	}

	var state servicestate.ServiceState
	if err := LoadServiceState(conn, &state, serviceID, ssID); err != nil {
		return err
	}

	*rs = *NewRunningService(&service, &state)
	return nil
}

// LoadRunningServicesByHost gets all of the running services via a hostID lookup
func (z *Zookeeper) LoadRunningServicesByHost(rss *[]*dao.RunningService, hostIDs ...string) error {
	return z.call(func(conn client.Connection) error {
		return LoadRunningServicesByHost(conn, rss, hostIDs...)
	})
}

// LoadRunningServicesByHost gets all of the running services via a hostID lookup
func LoadRunningServicesByHost(conn client.Connection, rss *[]*dao.RunningService, hostIDs ...string) error {
	for _, hostID := range hostIDs {
		msg := newHostServiceStateMessage(nil, hostID, "")
		err := children(conn, msg.path, func(ssID string) error {
			var hss HostServiceState
			if err := LoadHostServiceState(conn, &hss, hostID, ssID); err != nil {
				return err
			}

			var service service.Service
			if err := LoadService(conn, &service, hss.ServiceID); err != nil {
				return err
			}

			var state servicestate.ServiceState
			if err := LoadServiceState(conn, &state, hss.ServiceID, hss.ServiceStateID); err != nil {
				return err
			}

			*rss = append(*rss, NewRunningService(&service, &state))
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// LoadRunningServicesByService gets all of the running services via a serviceID lookup
func (z *Zookeeper) LoadRunningServicesByService(rss *[]*dao.RunningService, serviceIDs ...string) error {
	return z.call(func(conn client.Connection) error {
		return LoadRunningServicesByService(conn, rss, serviceIDs...)
	})
}

// LoadRunningServicesByService gets all of the running services via a serviceID lookup
func LoadRunningServicesByService(conn client.Connection, rss *[]*dao.RunningService, serviceIDs ...string) error {
	for _, serviceID := range serviceIDs {
		var service service.Service
		if err := LoadService(conn, &service, serviceID); err != nil {
			return err
		}

		var states []*servicestate.ServiceState
		if err := LoadServiceStates(conn, &states, serviceID); err != nil {
			return err
		}

		for _, state := range states {
			*rss = append(*rss, NewRunningService(&service, state))
		}
	}
	return nil
}

// LoadRunningServices gets all of the running services
func (z *Zookeeper) LoadRunningServices(rss *[]*dao.RunningService) error {
	return z.call(func(conn client.Connection) error {
		return LoadRunningServices(conn, rss)
	})
}

// LoadRunningServices gets all of the running services
func LoadRunningServices(conn client.Connection, rss *[]*dao.RunningService) error {
	var serviceIDs []string
	err := children(conn, zkService, func(serviceID string) error {
		serviceIDs = append(serviceIDs, serviceID)
		return nil
	})
	if err != nil {
		return err
	}
	return LoadRunningServicesByService(conn, rss, serviceIDs...)
}