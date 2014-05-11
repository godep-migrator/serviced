package zzk

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
)

type message struct {
	Payload interface{}
	path    string
	version interface{}
}

func newMessage(payload interface{}, nodes ...string) *message {
	return &message{
		Payload: payload,
		path:    path.Join(nodes...),
	}
}

func (m *message) Version() interface{}           { return m.version }
func (m *message) SetVersion(version interface{}) { m.version = version }

// Zookeeper sends payloads to the zookeeper server
type Zookeeper struct {
	client *client.Client
}

// New initializes a new zookeeper construct
func New(client *client.Client) *Zookeeper {
	return &Zookeeper{
		client: client,
	}
}

func (z *Zookeeper) getW(f func(client.Connection) (<-chan client.Event, error)) (<-chan client.Event, error) {
	conn, err := z.client.GetConnection()
	if err != nil {
		glog.Errorf("Error connecting to client: %s", err)
		return nil, err
	}
	defer conn.Close()
	return f(conn)
}

func (z *Zookeeper) call(f func(client.Connection) error) error {
	conn, err := z.client.GetConnection()
	if err != nil {
		glog.Errorf("Error connecting to client: %s", err)
		return err
	}
	defer conn.Close()
	return f(conn)
}

func getW(conn client.Connection, msg *message) (<-chan client.Event, error) {
	event, err := conn.GetW(msg.path, msg)
	if err != nil {
		glog.Errorf("Unable to retrieve message watch at %s: %s", msg.path, err)
	}
	return event, nil
}

func childrenW(conn client.Connection, path string) (<-chan client.Event, error) {
	// Make the path if it doesn't exist
	if err := mkdir(conn, path); err != nil {
		return nil, err
	}
	// Get the event channel
	_, event, err := conn.ChildrenW(path)
	if err != nil {
		glog.Errorf("Unable to retrieve child watch at %s: %s", path, err)
		return nil, err
	}
	return event, err
}

func get(conn client.Connection, msg *message) error {
	if err := conn.Get(msg.path, msg); err != nil {
		glog.Errorf("Unable to retrieve message at %s: %s", msg.path, err)
		return err
	}
	return nil
}

func children(conn client.Connection, path string, f func(string) error) error {
	nodes, err := conn.Children(path)
	if err != nil {
		glog.Errorf("Unable to retrieve children at %s: %s", path, err)
		return err
	}
	for _, node := range nodes {
		if err := f(node); err != nil {
			return err
		}
	}
	return nil
}

func mkdir(conn client.Connection, dpath string) error {
	if exists, err := conn.Exists(dpath); err != nil && err != client.ErrNoNode {
		glog.Errorf("Error checking path %s: %s", dpath, err)
		return err
	} else if exists {
		return nil
	} else if mkdir(conn, path.Dir(dpath)); err != nil {
		return err
	}
	return conn.CreateDir(dpath)
}

func add(conn client.Connection, msg *message) error {
	if err := mkdir(conn, path.Dir(msg.path)); err != nil {
		return err
	}

	if err := conn.Create(msg.path, msg); err != nil {
		glog.Errorf("Unable to create a message at %s: %s", msg.path, err)
		return err
	}

	glog.V(0).Infof("Added message at %s", msg.path)
	return nil
}

func update(conn client.Connection, msg *message) error {
	// if node does not exist, create
	if exists, err := conn.Exists(msg.path); err != nil && err != client.ErrNoNode {
		return err
	} else if !exists {
		return add(conn, msg)
	}

	if err := conn.Get(msg.path, msg); err != nil {
		return err
	}

	glog.V(0).Infof("Upating message at %s: %+v", msg.path, msg.Payload)
	return conn.Set(msg.path, msg)
}

func remove(conn client.Connection, msg *message) error {
	// if the node does not exist, error
	if exists, err := conn.Exists(msg.path); err != nil && err != client.ErrNoNode {
		glog.Errorf("Unable to get node for deletion %s: %s", msg.path, err)
		return err
	} else if !exists {
		return nil
	}
	if err := conn.Delete(msg.path); err != nil {
		glog.Errorf("Unable to delete node %s: %s", msg.path, err)
		return err
	}
	return nil
}