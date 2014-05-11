package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/service"
)

const (
	zkService = "/services"
)

func newServiceMessage(service *service.Service, serviceID string) *message {
	var err error
	if serviceID == "" {
		serviceID, err = newuuid()
		if err != nil {
			panic(err)
		}
	}
	return newMessage(service, zkService, serviceID)
}

// LoadServiceW returns a watch event that monitors a particular service
func (z *Zookeeper) LoadServiceW(service *service.Service, serviceID string) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadServiceW(conn, service, serviceID)
	})
}

// LoadServiceW returns a watch event that monitors a particular service
func LoadServiceW(conn client.Connection, service *service.Service, serviceID string) (<-chan client.Event, error) {
	msg := newServiceMessage(service, serviceID)
	return getW(conn, msg)
}

// LoadServicesW returns a watch event that monitors all services
func (z *Zookeeper) LoadServicesW(services *[]*service.Service) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadServicesW(conn, services)
	})
}

// LoadServicesW returns a watch event that monitors all services
func LoadServicesW(conn client.Connection, services *[]*service.Service) (<-chan client.Event, error) {
	if err := LoadServices(conn, services); err != nil {
		return nil, err
	}
	return childrenW(conn, zkService)
}

// LoadService loads a particular service
func (z *Zookeeper) LoadService(service *service.Service, serviceID string) error {
	return z.call(func(conn client.Connection) error {
		return LoadService(conn, service, serviceID)
	})
}

// LoadService loads a particular service
func LoadService(conn client.Connection, service *service.Service, serviceID string) error {
	msg := newServiceMessage(service, serviceID)
	return get(conn, msg)
}

// LoadServices loads all services
func (z *Zookeeper) LoadServices(services *[]*service.Service) error {
	return z.call(func(conn client.Connection) error {
		return LoadServices(conn, services)
	})
}

// LoadServices loads all services
func LoadServices(conn client.Connection, services *[]*service.Service) error {
	serviceMap := make(map[string]*service.Service)
	err := children(conn, zkService, func(serviceID string) error {
		if _, ok := serviceMap[serviceID]; !ok {
			var service service.Service
			if err := LoadService(conn, &service, serviceID); err != nil {
				return err
			}
			serviceMap[serviceID] = &service
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, s := range serviceMap {
		*services = append(*services, s)
	}
	return nil
}

// AddService creates a new service
func (z *Zookeeper) AddService(service *service.Service) error {
	return z.call(func(conn client.Connection) error {
		return AddService(conn, service)
	})
}

// AddService creates a new service
func AddService(conn client.Connection, service *service.Service) error {
	msg := newServiceMessage(service, "")
	return add(conn, msg)
}

// UpdateService updates a given service
func (z *Zookeeper) UpdateService(service *service.Service) error {
	return z.call(func(conn client.Connection) error {
		return UpdateService(conn, service)
	})
}

// UpdateService updates a given service
func UpdateService(conn client.Connection, service *service.Service) error {
	msg := newServiceMessage(service, service.Id)
	return update(conn, msg)
}

// RemoveService removes a service
func (z *Zookeeper) RemoveService(serviceID string) error {
	return z.call(func(conn client.Connection) error {
		return RemoveService(conn, serviceID)
	})
}

// RemoveService removes a service
func RemoveService(conn client.Connection, serviceID string) error {
	msg := newServiceMessage(nil, serviceID)
	return remove(conn, msg)
}

// LoadAndUpdateService loads a service and mutates the values
func (z *Zookeeper) LoadAndUpdateService(serviceID string, mutate func(*service.Service)) error {
	return z.call(func(conn client.Connection) error {
		return LoadAndUpdateService(conn, serviceID, mutate)
	})
}

// LoadAndUpdateService loads a service and mutates the values
func LoadAndUpdateService(conn client.Connection, serviceID string, mutate func(*service.Service)) error {
	var service service.Service
	if err := LoadService(conn, &service, serviceID); err != nil {
		return err
	}
	mutate(&service)
	return UpdateService(conn, &service)
}