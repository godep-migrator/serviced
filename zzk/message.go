package zzk

import (
	"path"
)

const (
	zkHome     = "/"
	zkRequest  = "/req"
	zkResponse = "/res"
)

// Message holds the payload that is sent to the client
type Message struct {
	Payload interface{}

	home    string
	id      string
	version interface{}
}

// NewMessage initializes a new message
func NewMessage(id string, payload interface{}) Message {
	var err error

	if id == "" {
		if id, err = newuuid(); err != nil {
			panic(err)
		}
	}

	m := Message{
		home:    zkHome,
		id:      id,
		Payload: payload,
	}
	return m
}

// Version returns the version
func (m *Message) Version() interface{} { return m.version }

// SetVersion sets the version
func (m *Message) SetVersion(version interface{}) { m.version = version }

// Path returns the path of the message on zookeeper
func (m *Message) Path() string { return path.Join(m.home, m.id) }