package zzk

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
)

// Zookeeper sends payloads to the zookeeper server
type Zookeeper struct {
	client client.Client
}

// New initializes a new zookeeper construct
func New(client client.Client) *Zookeeper {
	return &Zookeeper{
		client: client,
	}
}

func (z *Zookeeper) getW(msg *Message, f func(client.Connection, *Message) (<-chan client.Event, error)) (<-chan client.Event, error) {
	conn, err := z.client.GetConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return f(conn, msg)
}

func (z *Zookeeper) call(msg *Message, f func(client.Connection, *Message) error) error {
	conn, err := z.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	return f(conn, msg)
}

func getW(conn client.Connection, msg *Message) (<-chan client.Event, error) {
	p := msg.Path()
	event, err := conn.GetW(p, msg)
	if err != nil {
		glog.Errorf("Unable to retrieve message watch at %s: %s", p, err)
	}
	return event, nil
}

func childrenW(conn client.Connection, msg *Message) (<-chan client.Event, error) {
	p := msg.Path()
	nodes, event, err := conn.ChildrenW(p)
	if err != nil {
		glog.Errorf("Unable retrieve child watch at %s: %s", p, err)
	}
	msg.Payload = &nodes
	return event, nil
}

func children(conn client.Connection, msg *Message) error {
	p := msg.Path()
	nodes, err := conn.Children(p)
	if err != nil {
		glog.Errorf("Unable to retrieve children at %s: %s", p, err)
		return err
	}
	msg.Payload = &nodes
	return nil
}

func get(conn client.Connection, msg *Message) error {
	p := msg.Path()
	if err := conn.Get(p, msg); err != nil {
		glog.Errorf("Unable to retrieve message at %s: %s", p, err)
		return err
	}
	return nil
}

func mkdir(conn client.Connection, msg *Message) error {
	var dir func(string) error
	dir = func(p string) error {
		if exists, err := conn.Exists(p); err != nil && err != client.ErrNoNode {
			return err
		} else if exists {
			return nil
		} else if dir(path.Dir(p)); err != nil {
			return err
		}
		return conn.CreateDir(p)
	}
	return dir(msg.Path())
}

func add(conn client.Connection, msg *Message) error {
	p := msg.Path()
	if err := mkdir(conn, &Message{home: path.Dir(p)}); err != nil {
		return err
	}

	if err := conn.Create(p, msg); err != nil {
		glog.Errorf("Unable to create a message at %s: %s", p, err)
		return err
	}

	glog.V(0).Infof("Added message at %s", p)
	return nil
}

func update(conn client.Connection, msg *Message) error {
	// if node does not exist, create
	p := msg.Path()
	exists, err := conn.Exists(p)
	if err != nil && err != client.ErrNoNode {
		return err
	} else if !exists {
		return add(conn, msg)
	}

	if err := conn.Get(p, msg); err != nil {
		return err
	}

	glog.V(0).Infof("Upating message at %s: %+v", p, msg.Payload)
	return conn.Set(p, msg)
}

func remove(conn client.Connection, msg *Message) error {
	p := msg.Path()
	if err := conn.Delete(p); err != nil {
		glog.Errorf("Unable to delete message at %s: %+v", p, err)
		return err
	}
	return nil
}