package zzk

import (
	"github.com/zenoss/serviced/coordinator/client"
)

const (
	zkSnapshot = "/snapshots"
)

type SnapshotRequest struct {
	ServiceID string
	Requestor string
}

type SnapshotResponse struct {
	ID string
	ServiceID string
	Error error
}

func NewSnapshotRequest(id string, snapshot *SnapshotRequest) Request {
	msg := NewRequest(id, snapshot)
	msg.home = zkSnapshot
	return msg
}

func NewSnapshotResponse(reqID string, snapshot *SnapshotResponse) Response {
	msg := NewResponse(reqID, snapshot)
	msg.home = zkSnapshot
	return msg
}

func (z *Zookeeper) WatchSnapshotRequests() (<-chan client.Event, error) {
	msg := NewMessage(zkRequest, nil)
	msg.home = zkSnapshot
	if err := z.mkdir(msg.Path()); err != nil {
		return nil, err
	}
	return z.getW(&msg)
}

func (z *Zookeeper) LoadSnapshotRequestW(snapshot *SnapshotRequest, id string) error {
	msg := NewSnapshotRequest(id, snapshot)
	return z.getW(&msg)
}

func (z *Zookeeper) LoadSnapshotRequest(snapshot *SnapshotRequest, id string) error {
	msg := NewSnapshotRequest(id, snapshot)
	return z.call(&msg, get)
}

func (z *Zookeeper) LoadSnapshotRequests(snapshots *[]*SnapshotRequest) error {
	msg := NewMessage(zkRequest, nil)
	msg.home = zkSnapshot
	ids, err := z.children(&msg)
	if err != nil {
		return err
	}
	*snapshots = make([]*SnapshotRequest, len(ids))
	for i, id := range ids {
		var snapshot SnapshotRequest
		if err := z.LoadSnapshotRequest(&snapshot, id); err != nil {
			return err
		}
		(*snapshots)[i] = snapshot
	}
	return nil
}

func (z *Zookeeper) AddSnapshotRequest(snapshot *SnapshotRequest) error {
	req := NewSnapshotRequest("", snapshot)
	return z.call(&req, add)
}

func (z *Zookeeper) RemoveSnapshotRequest
func (z *Zookeeper) LoadSnapshotResponse
func (z *Zookeeper) AddSnapshotResponse
func (z *Zookeeper) RemoveSnapshotResponse

func (z *Zookeeper) LoadSnapshotW(snapshot *Snapshot, id string) (<-chan client.Event, error) {
	msg := NewSnapshotMessage(id, snapshot)
	return z.getW(&msg)
}

func (z *Zookeeper) LoadSnapshot(snapshot *Snapshot, id string) error {
	msg := NewSnapshotMessage(id, snapshot)
	return z.call(&msg, get)
}

func (z *Zookeeper) AddSnapshot(snapshot *Snapshot) error {
	msg := NewSnapshotMessage(snapshot.ID, snapshot)
	return z.call(&msg, add)
}

func (z *Zookeeper) UpdateSnapshot(snapshot *Snapshot) error {
	msg := NewSnapshotMessage(snapshot.ID, snapshot)
	return z.call(&msg, update)
}

func (z *Zookeeper) LoadAndUpdateSnapshot(id string, mutate func(*Snapshot)) error {
	var snapshot Snapshot
	if err := z.LoadSnapshot(&snapshot, id); err != nil {
		return err
	}
	mutate(&snapshot)
	return z.UpdateSnapshot(&snapshot)
}

func (z *Zookeeper) UpdateSnapshotResult(id string, err error) error {
	return z.LoadAndUpdateSnapshot(id, func(s *Snapshot) {
		(*s).Error = err
	})
}

func (z *Zookeeper) RemoveSnapshot(id string) error {
	msg := NewSnapshotMessage(id, nil)
	return z.call(&msg, remove)
}