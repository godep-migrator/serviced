package snapshot

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
)

const (
	zkSnapshot = "/snapshots"
)

func snapshotpath(nodes ...string) string {
	p := []string{zkSnapshot}
	p = append(p, nodes...)
	return path.Join(p...)
}

// Snapshot is the snapshot request object
type Snapshot struct {
	ServiceID string
	Label     string
	Error     error
	version   interface{}
}

// Version implements client.Node
func (s *Snapshot) Version() interface{} { return s.version }

// SetVersion implements client.Node
func (s *Snapshot) SetVersion(version interface{}) { s.version = version }

func (s *Snapshot) done() bool { return s.Label != "" || s.Error != nil }

type SnapshotHandler interface {
	TakeSnapshot(serviceID string) (string, error)
}

type SnapshotListener struct {
	conn    client.Connection
	handler SnapshotHandler
}

func NewSnapshotListener(conn client.Connection, handler SnapshotHandler) *SnapshotListener {
	return &SnapshotListener{conn, handler}
}

// Listen listens for changes on the event node and processes the snapshot
func (l *SnapshotListener) Listen() {
	// Make the path if it doesn't exist
	if exists, err := l.conn.Exists(snapshotpath()); err != nil && err != client.ErrNoNode {
		glog.Errorf("Error checking path %s: %s", snapshotpath(), err)
		return
	} else if !exists {
		if err := l.conn.CreateDir(snapshotpath()); err != nil {
			glog.Errorf("Could not create path %s: %s", snapshotpath(), err)
			return
		}
	}

	// Wait for snapshot events
	for {
		nodes, event, err := l.conn.ChildrenW(snapshotpath())
		if err != nil {
			glog.Errorf("Could not watch snapshots: %s", err)
			return
		}

		for _, serviceID := range nodes {
			// Get the request
			path := snapshotpath(serviceID)
			var snapshot Snapshot
			if err := l.conn.Get(path, &snapshot); err != nil {
				glog.V(1).Infof("Could not get snapshot %s: %s", serviceID, err)
				continue
			}

			// Snapshot action already performed, continue
			if snapshot.done() {
				continue
			}

			// Do snapshot
			glog.V(1).Infof("Taking snapshot for request: %v", snapshot)
			snapshot.Label, snapshot.Error = l.handler.TakeSnapshot(snapshot.ServiceID)
			if snapshot.Error != nil {
				glog.V(1).Infof("Snapshot failed for request: %v", snapshot)
			}
			// Update request
			if err := l.conn.Set(path, &snapshot); err != nil {
				glog.V(1).Infof("Could not update snapshot request %s: %s", serviceID, err)
				continue
			}

			glog.V(1).Infof("Finished taking snapshot for request: %v", snapshot)
		}
		// Wait for an event that something changed
		<-event
	}
}

// SendSnapshot sends a new snapshot request to the queue
func SendSnapshot(conn client.Connection, snapshot *Snapshot) error {
	return conn.Create(snapshotpath(snapshot.ServiceID), snapshot)
}

// WaitSnapshot waits for a snapshot to be complete and then deletes the request
func WaitSnapshot(conn client.Connection, serviceID string) (Snapshot, error) {
	var snapshot Snapshot
	node := snapshotpath(serviceID)

	for {
		event, err := conn.GetW(node, &snapshot)
		if err != nil {
			return snapshot, err
		}
		if snapshot.done() {
			// Delete the request
			if err := conn.Delete(node); err != nil {
				glog.Warningf("Could not delete snapshot request %s: %s", node, err)
			}
			return snapshot, nil
		}
		// Wait for something to happen
		<-event
	}
}