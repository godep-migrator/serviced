package service

import (
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"
)

func NewRunningService(service *service.Service, state *servicestate.ServiceState) *dao.RunningService {
	return &dao.RunningService{
		Id:              state.Id,
		ServiceID:       state.ServiceID,
		StartedAt:       state.Started,
		HostID:          state.HostID,
		DockerID:        state.DockerID,
		InstanceID:      state.InstanceID,
		Startup:         service.Startup,
		Name:            service.Name,
		Description:     service.Description,
		Instances:       service.Instances,
		PoolID:          service.PoolID,
		ImageID:         service.ImageID,
		DesiredState:    service.DesiredState,
		ParentServiceID: service.ParentServiceID,
		RAMCommitment:   service.RAMCommitment,
	}
}

func LoadRunningService(conn client.Connection, serviceID, ssID string) (*dao.RunningService, error) {
	var service service.Service
	if err := conn.Get(servicepath(serviceID), &service); err != nil {
		return nil, err
	}

	var state servicestate.ServiceState
	if err := conn.Get(servicepath(serviceID, ssID), &state); err != nil {
		return nil, err
	}

	return NewRunningService(&service, &state), nil
}

func LoadRunningServicesByHost(conn client.Connection, hostIDs ...string) ([]*dao.RunningService, error) {
	var rss []*dao.RunningService
	for _, hostID := range hostIDs {
		stateIDs, err := conn.Children(hostpath(hostID))
		if err != nil {
			return nil, err
		}
		for _, ssID := range stateIDs {
			var hs HostState
			if err := conn.Get(hostpath(hostID, ssID), &hs); err != nil {
				return nil, err
			}

			rs, err := LoadRunningService(conn, hs.ServiceID, hs.ID)
			if err != nil {
				return nil, err
			}

			rss = append(rss, rs)
		}
	}
	return rss, nil
}

func LoadRunningServicesByService(conn client.Connection, serviceIDs ...string) ([]*dao.RunningService, error) {
	var rss []*dao.RunningService
	for _, serviceID := range serviceIDs {
		stateIDs, err := conn.Children(servicepath(serviceID))
		if err != nil {
			return nil, err
		}
		for _, ssID := range stateIDs {
			rs, err := LoadRunningService(conn, serviceID, ssID)
			if err != nil {
				return nil, err
			}
			rss = append(rss, rs)
		}
	}
	return rss, nil
}

func LoadRunningServices(conn client.Connection) ([]*dao.RunningService, error) {
	serviceIDs, err := conn.Children(servicepath())
	if err != nil {
		return nil, err
	}

	// filter non-unique service ids
	var (
		unique map[string]interface{}
		ids    []string
	)
	for _, serviceID := range serviceIDs {
		if _, ok := unique[serviceID]; !ok {
			unique[serviceID] = nil
			ids = append(ids, serviceID)
		}
	}

	return LoadRunningServicesByService(conn, ids...)
}