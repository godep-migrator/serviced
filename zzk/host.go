package zzk

import (
	"github.com/zenoss/serviced/domain/host"
)

const (
	zkHost = "/hosts"
)

func NewHostMessage(id string, host *host.Host) Message {
	msg := NewMessage(id, host)
	msg.home = zkHost
	return msg
}