package zzk

import (
	"path"
)

const (
	zkHome     = "/"
	zkRequest  = "/req"
	zkResponse = "/res"
)

type Node interface {
	Path() string
	Version() int32
	SetVersion(int32)
}

// Message holds the payload that is sent to the client
type Message struct {
	Payload interface{}

	home    string
	id      string
	version int32
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
func (m *Message) Version() int32 { return m.version }

// SetVersion sets the version
func (m *Message) SetVersion(version int32) { m.version = version }

// Path returns the path of the message on zookeeper
func (m *Message) Path() string { return path.Join(m.home, m.id) }

// Request is the zookeeper request message
type Request struct {
	Message
}

// NewRequest initializes a new request
func NewRequest(payload interface{}) Request {
	return Request{
		Message: NewMessage("", payload),
	}
}

// Path returns the path of the message on zookeeper
func (m *Request) Path() string { return path.Join(m.home, zkRequest, m.id) }

// Response is the zookeeper response message
type Response struct {
	Message
}

// NewResponse initializes a new response
func NewResponse(reqID string, payload interface{}) Response {
	return Response{
		Message: NewMessage(reqID, payload),
	}
}

// Path returns the path of the message on zookeeper
func (m *Response) Path() string { return path.Join(m.home, zkResponse, m.id) }