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

// NewSnapshotMessage builds a new snapshot message
func NewSnapshotMessage(serviceID string, snapshot *Snapshot) Message {
	msg := NewMessage(serviceID, snapshot)
	msg.home = zkSnapshot
	return msg
}

// LoadSnapshotsW returns an event channel that indicates when there is a change to the snapshots node
func (z *Zookeeper) LoadSnapshotsW(snapshots *[]*Snapshot) (<-chan client.Event, error) {
	var reqids []string
	msg := Message{
		home:    zkSnapshot,
		Payload: &reqids,
	}

	event, err := z.getW(&msg, childrenW)
	if err != nil {
		return nil, err
	}
	go func() {
		<-event
		*snapshots = make([]*Snapshot, len(reqids))
		for i, rid := range reqids {
			var snapshot Snapshot
			if err := z.LoadSnapshot(&snapshot, rid); err != nil {
				return
			}
			*(*snapshots)[i] = snapshot
		}
	}()
	return event, err
}

// LoadSnapshotW returns an event channel that indicates when there is a change to a specific snapshot
func (z *Zookeeper) LoadSnapshotW(snapshot *Snapshot, id string) (<-chan client.Event, error) {
	msg := NewSnapshotMessage(id, snapshot)
	return z.getW(&msg, getW)
}

// LoadSnapshot loads a particular snaphot
func (z *Zookeeper) LoadSnapshot(snapshot *Snapshot, id string) error {
	msg := NewSnapshotMessage(id, snapshot)
	return z.call(&msg, get)
}

// LoadSnapshots loads all of the snapshots
func (z *Zookeeper) LoadSnapshots(snapshots *[]*Snapshot) error {
	var reqids []string
	msg := Message{
		home:    zkSnapshot,
		Payload: &reqids,
	}
	if err := z.call(&msg, children); err != nil {
		return err
	}

	*snapshots = make([]*Snapshot, len(reqids))
	for i, id := range reqids {
		var snapshot Snapshot
		if err := z.LoadSnapshot(&snapshot, id); err != nil {
			return err
		}
		*(*snapshots)[i] = snapshot
	}
	return nil
}

// AddSnapshot creates a new snapshot request
func (z *Zookeeper) AddSnapshot(snapshot *Snapshot) error {
	msg := NewSnapshotMessage(snapshot.ServiceID, snapshot)
	return z.call(&msg, add)
}

// UpdateSnapshot updates the existing snapshot request
func (z *Zookeeper) UpdateSnapshot(snapshot *Snapshot, id string) error {
	msg := NewSnapshotMessage(id, snapshot)
	return z.call(&msg, add)
}

// RemoveSnapshot removes an existing snapshot request
func (z *Zookeeper) RemoveSnapshot(id string) error {
	msg := NewSnapshotMessage(id, nil)
	return z.call(&msg, remove)
}