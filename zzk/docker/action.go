package docker

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/utils"
)

const (
	zkAction = "/docker/action"
)

func actionPath(nodes ...string) string {
	p := []string{zkAction}
	p = append(p, nodes...)
	return path.Join(p...)
}

// Action is the request node for initialized a serviced action on a host
type Action struct {
	HostID   string
	DockerID string
	Command  []string
	Started  bool
	version  interface{}
}

// Version is an implementation of client.Node
func (a *Action) Version() interface{} { return a.version }

// SetVersion is an implementation of client.Node
func (a *Action) SetVersion(version interface{}) { a.version = version }

// ActionListener is the listener object for /docker/actions
type ActionListener struct {
	conn   client.Connection
	hostID string
}

// NewActionListener instantiates a new action listener for /docker/actions
func NewActionListener(conn client.Connection, hostID string) *ActionListener {
	return &ActionListener{conn, hostID}
}

// Listen listens for new actions for a particular host
func (l *ActionListener) Listen() {
	// Make the path if it doesn't exist
	node := actionPath(l.hostID)
	if err := l.conn.CreateDir(node); err != nil {
		glog.Errorf("Could not create path %s: %s", node, err)
		return
	}

	// Wait for action commands
	for {
		nodes, event, err := l.conn.ChildrenW(node)
		if err != nil {
			glog.Errorf("Could not listen for commands %s: %s", node, err)
			return
		}

		for _, id := range nodes {
			// Get the request
			path := actionPath(l.hostID, id)
			var action Action
			if err := l.conn.Get(path, &action); err != nil {
				glog.V(1).Infof("Could not get action at %s: %s", path, err)
				continue
			}

			// action already started, continue
			if action.Started {
				continue
			}

			// do action
			glog.V(1).Infof("Performing action to service state via request: %v", &action)
			action.Started = true
			if err := l.conn.Set(path, &action); err != nil {
				glog.Warningf("Could not update command at %s", path, err)
				continue
			}

			go func() {
				defer l.conn.Delete(path)
				result, err := utils.RunNSInitWithRetry(action.DockerID, action.Command)
				if result != nil && len(result) > 0 {
					glog.Info(string(result))
				}
				if err != nil {
					glog.Warningf("Error running command `%s` on container %s: %s", action.Command, action.DockerID, err)
				} else {
					glog.V(1).Infof("Successfully ran command `%s` on container %s", action.Command, action.DockerID)
				}
			}()
		}

		// wait for an event that something changed
		<-event
	}
}

// SendAction sends an action request to a particular host
func SendAction(conn client.Connection, action *Action) (string, error) {
	uuid, err := utils.NewUUID()
	if err != nil {
		return "", err
	}

	node := actionPath(action.HostID, uuid)
	if err := conn.Create(node, action); err != nil {
		return "", err
	}
	return path.Base(node), nil
}