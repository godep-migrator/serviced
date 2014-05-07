package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/service"
)

const (
	zkService = "/services"
)

// NewServiceMessage returns a message for a service
func NewServiceMessage(id string, service *service.Service) Message {
	msg := NewMessage(id, service)
	msg.home = zkService
	return msg
}

// LoadServiceW returns a watch event that monitors a particular service
func (z *Zookeeper) LoadServiceW(service *service.Service, id string) (<-chan client.Event, error) {
	msg := NewServiceMessage(id, service)
	return z.getW(&msg)
}

// LoadService loads a particular service
func (z *Zookeeper) LoadService(service *service.Service, id string) error {
	msg := NewServiceMessage(id, service)
	return z.call(&msg, get)
}

// AddService creates a new service
func (z *Zookeeper) AddService(service *service.Service) error {
	msg := NewServiceMessage("", service)
	return z.call(&msg, add)
}

// UpdateService updates a given service
func (z *Zookeeper) UpdateService(service *service.Service) error {
	msg := NewServiceMessage(service.Id, service)
	return z.call(&msg, update)
}

// RemoveService removes a service
func (z *Zookeeper) RemoveService(id string) error {
	msg := NewServiceMessage(id, nil)
	return z.call(&msg, remove)
}

// LoadAndUpdateService loads a service and mutates the values
func (z *Zookeeper) LoadAndUpdateService(id string, mutate func(*service.Service)) error {
	var service service.Service
	if err := z.LoadService(&service, id); err != nil {
		return err
	}
	mutate(&service)
	return z.UpdateService(&service)
}