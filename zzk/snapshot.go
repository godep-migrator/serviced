package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
)

const (
	zkSnapshot = "/snapshots"
)

// Snapshot is the payload to be attached to the message
type Snapshot struct {
	ServiceID string
	Requestor string
	Label     string
	Error     error
}

// Done is an indicator to determine if a snapshot needs to be performed
func (s Snapshot) Done() bool {
	return s.Label != "" || s.Error != nil
}

func newSnapshotMessage(snapshot *Snapshot, serviceID string) *message {
	return newMessage(snapshot, zkSnapshot, serviceID)
}

// LoadSnapshotW returns an event channel that indicates when there is a change to a specific snapshot
func (z *Zookeeper) LoadSnapshotW(snapshot *Snapshot, serviceID string) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadSnapshotW(conn, snapshot, serviceID)
	})
}

// LoadSnapshotW returns an event channel that indicates when there is a change to a specific snapshot
func LoadSnapshotW(conn client.Connection, snapshot *Snapshot, serviceID string) (<-chan client.Event, error) {
	msg := newSnapshotMessage(snapshot, serviceID)
	return getW(conn, msg)
}

// LoadSnapshotsW returns an event channel when there are any snapshot changes
func (z *Zookeeper) LoadSnapshotsW(snapshots *[]*Snapshot) (<-chan client.Event, error) {
	return z.getW(func(conn client.Connection) (<-chan client.Event, error) {
		return LoadSnapshotsW(conn, snapshots)
	})
}

// LoadSnapshotsW returns an event channel when there are any snapshot changes
func LoadSnapshotsW(conn client.Connection, snapshots *[]*Snapshot) (<-chan client.Event, error) {
	if err := mkdir(conn, zkSnapshot); err != nil {
		return nil, err
	}

	if err := LoadSnapshots(conn, snapshots); err != nil {
		return nil, err
	}

	return childrenW(conn, zkSnapshot)
}

// LoadSnapshot loads a particular snaphot
func (z *Zookeeper) LoadSnapshot(snapshot *Snapshot, serviceID string) error {
	return z.call(func(conn client.Connection) error {
		return LoadSnapshot(conn, snapshot, serviceID)
	})
}

// LoadSnapshot loads a particular snaphot
func LoadSnapshot(conn client.Connection, snapshot *Snapshot, serviceID string) error {
	msg := newSnapshotMessage(snapshot, serviceID)
	return get(conn, msg)
}

// LoadSnapshots loads all of the snapshots
func (z *Zookeeper) LoadSnapshots(snapshots *[]*Snapshot) error {
	return z.call(func(conn client.Connection) error {
		return LoadSnapshots(conn, snapshots)
	})
}

// LoadSnapshots loads all of the snapshots
func LoadSnapshots(conn client.Connection, snapshots *[]*Snapshot) error {
	err := children(conn, zkSnapshot, func(serviceID string) error {
		var snapshot Snapshot
		if err := LoadSnapshot(conn, &snapshot, serviceID); err != nil {
			return err
		}
		*snapshots = append(*snapshots, &snapshot)
		return nil
	})
	return err
}

// AddSnapshot creates a new snapshot request
func (z *Zookeeper) AddSnapshot(snapshot *Snapshot) error {
	return z.call(func(conn client.Connection) error {
		return AddSnapshot(conn, snapshot)
	})
}

// AddSnapshot creates a new snapshot request
func AddSnapshot(conn client.Connection, snapshot *Snapshot) error {
	msg := newSnapshotMessage(snapshot, snapshot.ServiceID)
	return add(conn, msg)
}

// UpdateSnapshot updates the existing snapshot request
func (z *Zookeeper) UpdateSnapshot(snapshot *Snapshot) error {
	return z.call(func(conn client.Connection) error {
		return UpdateSnapshot(conn, snapshot)
	})
}

// UpdateSnapshot updates the existing snapshot request
func UpdateSnapshot(conn client.Connection, snapshot *Snapshot) error {
	msg := newSnapshotMessage(snapshot, snapshot.ServiceID)
	return update(conn, msg)
}

// RemoveSnapshot removes an existing snapshot request
func (z *Zookeeper) RemoveSnapshot(serviceID string) error {
	return z.call(func(conn client.Connection) error {
		return RemoveSnapshot(conn, serviceID)
	})
}

// RemoveSnapshot removes an existing snapshot request
func RemoveSnapshot(conn client.Connection, serviceID string) error {
	msg := newSnapshotMessage(nil, serviceID)
	return remove(conn, msg)
}