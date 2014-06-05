package zzk

import (
	"github.com/zenoss/glog"
	coordclient "github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicestate"

	"errors"
	"time"
)

const SERVICE_PATH = "/services"
const HOSTS_PATH = "/hosts"
const SCHEDULER_PATH = "/scheduler"
const SNAPSHOT_PATH = "/snapshots"
const SNAPSHOT_REQUEST_PATH = "/snapshots/requests"

type ZkDao struct {
	client *coordclient.Client
}

func NewZkDao(client *coordclient.Client) *ZkDao {
	return &ZkDao{
		client: client,
	}
}

type ZkConn struct {
	Conn coordclient.Connection
}

type HostServiceState struct {
	HostID         string
	ServiceID      string
	ServiceStateID string
	DesiredState   int
	version        interface{}
}

func (hss *HostServiceState) Version() interface{} {
	return hss.version
}

func (hss *HostServiceState) SetVersion(version interface{}) {
	hss.version = version
}

// Communicates to the agent that this service instance should stop
func TerminateHostService(conn coordclient.Connection, hostId string, serviceStateId string) error {
	return loadAndUpdateHss(conn, hostId, serviceStateId, func(hss *HostServiceState) {
		hss.DesiredState = service.SVCStop
	})
}

func ResetServiceState(conn coordclient.Connection, serviceId string, serviceStateId string) error {
	return LoadAndUpdateServiceState(conn, serviceId, serviceStateId, func(ss *servicestate.ServiceState) {
		ss.Terminated = time.Now()
	})
}

// Communicates to the agent that this service instance should stop
func (zkdao *ZkDao) TerminateHostService(hostId string, serviceStateId string) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		glog.Errorf("Unable to connect to zookeeper: %v", err)
		return err
	}
	defer conn.Close()

	return TerminateHostService(conn, hostId, serviceStateId)
}

func (zkdao *ZkDao) AddService(service *service.Service) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return AddService(conn, service)
}

type ServiceNode struct {
	Service *service.Service
	version interface{}
}

func (s *ServiceNode) Version() interface{} {
	return s.version
}

func (s *ServiceNode) SetVersion(version interface{}) {
	s.version = version
}

func AddService(conn coordclient.Connection, service *service.Service) error {
	glog.V(2).Infof("Creating new service %s", service.Id)

	svcNode := &ServiceNode{
		Service: service,
	}
	servicePath := ServicePath(service.Id)
	if err := conn.Create(servicePath, svcNode); err != nil {
		glog.Errorf("Unable to create service for %s: %v", servicePath, err)
	}

	glog.V(2).Infof("Successfully created %s", servicePath)
	return nil
}

type ServiceStateNode struct {
	ServiceState *servicestate.ServiceState
	version      interface{}
}

func (s *ServiceStateNode) Version() interface{} {
	return s.version
}

func (s *ServiceStateNode) SetVersion(version interface{}) {
	s.version = version
}

func (zkdao *ZkDao) AddServiceState(state *servicestate.ServiceState) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return AddServiceState(conn, state)
}

func AddServiceState(conn coordclient.Connection, state *servicestate.ServiceState) error {
	serviceStatePath := ServiceStatePath(state.ServiceID, state.Id)

	serviceStateNode := &ServiceStateNode{
		ServiceState: state,
	}

	if err := conn.Create(serviceStatePath, serviceStateNode); err != nil {
		glog.Errorf("Unable to create path %s because %v", serviceStatePath, err)
		return err
	}
	hostServicePath := HostServiceStatePath(state.HostID, state.Id)
	hss := SsToHss(state)
	if err := conn.Create(hostServicePath, hss); err != nil {
		glog.Errorf("Unable to create path %s because %v", hostServicePath, err)
		return err
	}
	return nil
}

func (zkdao *ZkDao) UpdateServiceState(state *servicestate.ServiceState) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	serviceStatePath := ServiceStatePath(state.ServiceID, state.Id)
	ssn := ServiceStateNode{}
	if err := conn.Get(serviceStatePath, &ssn); err != nil {
		return err
	}
	ssn.ServiceState = state
	return conn.Set(serviceStatePath, &ssn)
}

func (zkdao *ZkDao) UpdateService(service *service.Service) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	servicePath := ServicePath(service.Id)

	sn := ServiceNode{}
	if err := conn.Get(servicePath, &sn); err != nil {
		glog.V(3).Infof("ZkDao.UpdateService unexpectedly could not retrieve %s error:%v", servicePath, err)
		err = AddService(conn, service)
		return err
	}
	sn.Service = service
	glog.V(4).Infof("ZkDao.UpdateService %v, %v", servicePath, service)

	return conn.Set(servicePath, &sn)
}

func (zkdao *ZkDao) GetServiceState(serviceState *servicestate.ServiceState, serviceId string, serviceStateId string) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	return GetServiceState(conn, serviceState, serviceId, serviceStateId)
}

func GetServiceState(conn coordclient.Connection, serviceState *servicestate.ServiceState, serviceId string, serviceStateId string) error {
	serviceStateNode := ServiceStateNode{}
	err := conn.Get(ServiceStatePath(serviceId, serviceStateId), &serviceStateNode)
	if err != nil {
		return err
	}
	*serviceState = *serviceStateNode.ServiceState
	return nil
}

func (zkdao *ZkDao) GetServiceStates(serviceStates *[]*servicestate.ServiceState, serviceIds ...string) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return GetServiceStates(conn, serviceStates, serviceIds...)
}

func GetServiceStates(conn coordclient.Connection, serviceStates *[]*servicestate.ServiceState, serviceIds ...string) error {
	for _, serviceId := range serviceIds {
		err := appendServiceStates(conn, serviceId, serviceStates)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zkdao *ZkDao) GetRunningService(serviceId string, serviceStateId string, running *dao.RunningService) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	var s service.Service
	if err := LoadService(conn, serviceId, &s); err != nil {
		return err
	}

	var ss servicestate.ServiceState
	if err := LoadServiceState(conn, serviceId, serviceStateId, &ss); err != nil {
		return err
	}
	*running = *sssToRs(&s, &ss)
	return nil
}

func (zkdao *ZkDao) GetRunningServicesForHost(hostId string, running *[]*dao.RunningService) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	serviceStateIds, err := conn.Children(HostPath(hostId))
	if err != nil {
		glog.Errorf("Unable to acquire list of services")
		return err
	}

	_ss := make([]*dao.RunningService, len(serviceStateIds))
	for i, hssId := range serviceStateIds {

		var hss HostServiceState
		if err := LoadHostServiceState(conn, hostId, hssId, &hss); err != nil {
			return err
		}

		var s service.Service
		if err := LoadService(conn, hss.ServiceID, &s); err != nil {
			return err
		}

		var ss servicestate.ServiceState
		if err := LoadServiceState(conn, hss.ServiceID, hss.ServiceStateID, &ss); err != nil {
			return err
		}
		_ss[i] = sssToRs(&s, &ss)
	}
	*running = append(*running, _ss...)
	return nil
}

func (zkdao *ZkDao) GetRunningServicesForService(serviceId string, running *[]*dao.RunningService) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return LoadRunningServices(conn, running, serviceId)
}

func (zkdao *ZkDao) GetAllRunningServices(running *[]*dao.RunningService) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	serviceIds, err := conn.Children(SERVICE_PATH)
	if err != nil {
		glog.Errorf("Unable to acquire list of services")
		return err
	}
	return LoadRunningServices(conn, running, serviceIds...)
}

func HostPath(hostId string) string {
	return HOSTS_PATH + "/" + hostId
}

func ServicePath(serviceId string) string {
	return SERVICE_PATH + "/" + serviceId
}

func ServiceStatePath(serviceId string, serviceStateId string) string {
	return SERVICE_PATH + "/" + serviceId + "/" + serviceStateId
}

func HostServiceStatePath(hostId string, serviceStateId string) string {
	return HOSTS_PATH + "/" + hostId + "/" + serviceStateId
}

func (zkdao *ZkDao) RemoveService(id string) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return RemoveService(conn, id)
}

func RemoveService(conn coordclient.Connection, id string) error {
	glog.V(2).Infof("zkdao.RemoveService: %s - begin", id)
	defer glog.V(2).Infof("zkdao.RemoveService: %s - complete", id)

	servicePath := ServicePath(id)

	// First mark the service as needing to shutdown so the scheduler
	// doesn't keep trying to schedule new instances
	err := loadAndUpdateService(conn, id, func(s *service.Service) {
		s.DesiredState = service.SVCStop
	})
	if err != nil {
		return err
	} // Error already logged

	children, zke, err := conn.ChildrenW(servicePath)
	for ; err == nil && len(children) > 0; children, zke, err = conn.ChildrenW(servicePath) {

		select {

		case evt := <-zke:
			glog.V(1).Infof("RemoveService saw ZK event: %v", evt)
			continue

		case <-time.After(30 * time.Second):
			glog.V(0).Infof("Gave up deleting %s with %d children", servicePath, len(children))
			return errors.New("Timed out waiting for children to die for " + servicePath)
		}
	}
	if err != nil {
		glog.Errorf("Unable to get children for %s: %v", id, err)
		return err
	}

	var service service.Service
	if err := LoadService(conn, id, &service); err != nil {
		// Error already logged
		return err
	}
	if err := conn.Delete(servicePath); err != nil {
		glog.Errorf("Unable to delete service %s because: %v", servicePath, err)
		return err
	}
	glog.V(1).Infof("Service %s removed", servicePath)

	return nil
}

func RemoveServiceState(conn coordclient.Connection, serviceId string, serviceStateId string) error {
	ssPath := ServiceStatePath(serviceId, serviceStateId)

	var ss servicestate.ServiceState
	if err := LoadServiceState(conn, serviceId, serviceStateId, &ss); err != nil {
		return err
	} // Error already logged

	if err := conn.Delete(ssPath); err != nil {
		glog.Errorf("Unable to delete service state %s because: %v", ssPath, err)
		return err
	}

	hssPath := HostServiceStatePath(ss.HostID, serviceStateId)
	hss := HostServiceState{}
	if err := conn.Get(hssPath, &hss); err != nil {
		glog.Errorf("Unable to get host service state %s for delete because: %v", hssPath, err)
		return err
	}

	if err := conn.Delete(hssPath); err != nil {
		glog.Errorf("Unable to delete host service state %s", hssPath)
		return err
	}
	return nil
}

func LoadRunningServices(conn coordclient.Connection, running *[]*dao.RunningService, serviceIds ...string) error {
	for _, serviceId := range serviceIds {
		var s service.Service
		if err := LoadService(conn, serviceId, &s); err != nil {
			return err
		}

		servicePath := ServicePath(serviceId)
		childNodes, err := conn.Children(servicePath)
		if err != nil {
			return err
		}

		_ss := make([]*dao.RunningService, len(childNodes))
		for i, childId := range childNodes {
			var ss servicestate.ServiceState
			if err := LoadServiceState(conn, serviceId, childId, &ss); err != nil {
				return err
			}
			_ss[i] = sssToRs(&s, &ss)

		}
		*running = append(*running, _ss...)
	}
	return nil
}

func LoadHostServiceState(conn coordclient.Connection, hostId string, hssId string, hss *HostServiceState) error {
	hssPath := HostServiceStatePath(hostId, hssId)
	err := conn.Get(hssPath, hss)
	if err != nil {
		glog.Errorf("Unable to retrieve host service state %s: %v", hssPath, err)
		return err
	}
	return nil
}

func LoadHostServiceStateW(conn coordclient.Connection, hostId string, hssId string, hss *HostServiceState) (<-chan coordclient.Event, error) {
	hssPath := HostServiceStatePath(hostId, hssId)
	event, err := conn.GetW(hssPath, hss)
	if err != nil {
		glog.Errorf("Unable to retrieve host service state %s: %v", hssPath, err)
		return nil, err
	}
	return event, nil
}

func LoadService(conn coordclient.Connection, serviceId string, s *service.Service) error {
	sn := ServiceNode{}
	err := conn.Get(ServicePath(serviceId), &sn)
	if err != nil {
		glog.Errorf("Unable to retrieve service %s: %v", serviceId, err)
		return err
	}
	*s = *sn.Service
	return nil
}

func LoadServiceW(conn coordclient.Connection, serviceId string, s *service.Service) (<-chan coordclient.Event, error) {
	sn := ServiceNode{}
	event, err := conn.GetW(ServicePath(serviceId), &sn)
	if err != nil {
		//glog.Errorf("Unable to retrieve service %s: %v", serviceId, err)
		return nil, err
	}
	*s = *sn.Service
	return event, nil
}

func LoadServiceState(conn coordclient.Connection, serviceId string, serviceStateId string, ss *servicestate.ServiceState) error {
	ssPath := ServiceStatePath(serviceId, serviceStateId)
	ssn := ServiceStateNode{}
	err := conn.Get(ssPath, &ssn)
	if err != nil {
		glog.Errorf("Got error for %s: %v", ssPath, err)
		return err
	}
	*ss = *ssn.ServiceState
	return nil
}

func appendServiceStates(conn coordclient.Connection, serviceId string, serviceStates *[]*servicestate.ServiceState) error {
	servicePath := ServicePath(serviceId)
	childNodes, err := conn.Children(servicePath)
	if err != nil {
		return err
	}
	_ss := make([]*servicestate.ServiceState, len(childNodes))
	for i, childId := range childNodes {
		childPath := servicePath + "/" + childId
		ssn := ServiceStateNode{}
		err := conn.Get(childPath, &ssn)
		if err != nil {
			glog.Errorf("Got error for %s: %v", childId, err)
			return err
		}
		_ss[i] = ssn.ServiceState
	}
	*serviceStates = append(*serviceStates, _ss...)
	return nil
}

type serviceMutator func(*service.Service)
type hssMutator func(*HostServiceState)
type ssMutator func(*servicestate.ServiceState)

func LoadAndUpdateServiceState(conn coordclient.Connection, serviceId string, ssId string, mutator ssMutator) error {
	ssPath := ServiceStatePath(serviceId, ssId)

	ssn := ServiceStateNode{}
	err := conn.Get(ssPath, &ssn)
	if err != nil {
		// Should it really be an error if we can't find anything?
		glog.Errorf("Unable to find data %s: %v", ssPath, err)
		return err
	}
	mutator(ssn.ServiceState)
	if err := conn.Set(ssPath, &ssn); err != nil {
		glog.Errorf("Unable to update service state %s: %v", ssPath, err)
		return err
	}
	return nil
}

func loadAndUpdateService(conn coordclient.Connection, serviceId string, mutator serviceMutator) error {
	servicePath := ServicePath(serviceId)

	serviceNode := ServiceNode{}
	err := conn.Get(servicePath, &serviceNode)
	if err != nil {
		glog.Errorf("Unable to find data %s: %v", servicePath, err)
		return err
	}

	mutator(serviceNode.Service)
	if err := conn.Set(servicePath, &serviceNode); err != nil {
		glog.Errorf("Unable to update service %s: %v", servicePath, err)
		return err
	}
	return nil
}

func loadAndUpdateHss(conn coordclient.Connection, hostId string, hssId string, mutator hssMutator) error {
	hssPath := HostServiceStatePath(hostId, hssId)
	var hss HostServiceState

	err := conn.Get(hssPath, &hss)
	if err != nil {
		// Should it really be an error if we can't find anything?
		glog.Errorf("Unable to find data %s: %v", hssPath, err)
		return err
	}

	mutator(&hss)
	if err := conn.Set(hssPath, &hss); err != nil {
		glog.Errorf("Unable to update host service state %s: %v", hssPath, err)
		return err
	}
	return nil
}

// ServiceState to HostServiceState
func SsToHss(ss *servicestate.ServiceState) *HostServiceState {
	return &HostServiceState{
		HostID:         ss.HostID,
		ServiceID:      ss.ServiceID,
		ServiceStateID: ss.Id,
		DesiredState:   service.SVCRun,
	}
}

// Service & ServiceState to RunningService
func sssToRs(s *service.Service, ss *servicestate.ServiceState) *dao.RunningService {
	rs := &dao.RunningService{}
	rs.Id = ss.Id
	rs.ServiceID = ss.ServiceID
	rs.StartedAt = ss.Started
	rs.HostID = ss.HostID
	rs.DockerID = ss.DockerID
	rs.InstanceID = ss.InstanceID
	rs.Startup = s.Startup
	rs.Name = s.Name
	rs.Description = s.Description
	rs.Instances = s.Instances
	rs.PoolID = s.PoolID
	rs.ImageID = s.ImageID
	rs.DesiredState = s.DesiredState
	rs.ParentServiceID = s.ParentServiceID
	rs.MonitoringProfile = s.MonitoringProfile
	rs.MonitoringProfile.InstanceID = ss.InstanceID
	return rs
}

// Snapshot section start
func SnapshotRequestsPath(requestID string) string {
	return SNAPSHOT_REQUEST_PATH + "/" + requestID
}

func (zkdao *ZkDao) AddSnapshotRequest(snapshotRequest *dao.SnapshotRequest) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return AddSnapshotRequest(conn, snapshotRequest)
}

type SnapShotRequestNode struct {
	SnapshotRequest *dao.SnapshotRequest
	version         interface{}
}

func (s *SnapShotRequestNode) Version() interface{}           { return s.version }
func (s *SnapShotRequestNode) SetVersion(version interface{}) { s.version = version }

func AddSnapshotRequest(conn coordclient.Connection, snapshotRequest *dao.SnapshotRequest) error {
	glog.V(3).Infof("Creating new snapshot request %s", snapshotRequest.Id)

	// make sure toplevel paths exist
	paths := []string{SNAPSHOT_PATH, SNAPSHOT_REQUEST_PATH}
	for _, path := range paths {
		exists, err := conn.Exists(path)
		if err != nil {
			if err == coordclient.ErrNoNode {
				if err := conn.CreateDir(path); err != nil && err != coordclient.ErrNodeExists {
					return err
				}
			}
		}
		if !exists {
			if err := conn.CreateDir(path); err != nil && err != coordclient.ErrNodeExists {
				return err
			}
		}
	}

	// add the request to the snapshot request path
	srn := SnapShotRequestNode{
		SnapshotRequest: snapshotRequest,
	}
	snapshotRequestsPath := SnapshotRequestsPath(snapshotRequest.Id)
	if err := conn.Create(snapshotRequestsPath, &srn); err != nil {
		glog.Errorf("Unable to create snapshot request %s: %v", snapshotRequestsPath, err)
		return err
	}

	glog.V(0).Infof("Successfully created snapshot request %s", snapshotRequestsPath)
	return nil
}

func (zkdao *ZkDao) LoadSnapshotRequest(requestID string, sr *dao.SnapshotRequest) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return LoadSnapshotRequest(conn, requestID, sr)
}

func LoadSnapshotRequest(conn coordclient.Connection, requestID string, sr *dao.SnapshotRequest) error {

	srn := SnapShotRequestNode{}
	err := conn.Get(SnapshotRequestsPath(requestID), &srn)
	if err != nil {
		glog.Errorf("Unable to retrieve snapshot request %s: %v", requestID, err)
		return err
	}
	*sr = *srn.SnapshotRequest
	return nil
}

func (zkdao *ZkDao) LoadSnapshotRequestW(requestID string, sr *dao.SnapshotRequest) (<-chan coordclient.Event, error) {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return LoadSnapshotRequestW(conn, requestID, sr)
}

func LoadSnapshotRequestW(conn coordclient.Connection, requestID string, sr *dao.SnapshotRequest) (<-chan coordclient.Event, error) {
	srn := SnapShotRequestNode{}
	event, err := conn.GetW(SnapshotRequestsPath(requestID), &srn)
	if err != nil {
		glog.Errorf("Unable to retrieve snapshot request %s: %v", requestID, err)
		return nil, err
	}
	*sr = *srn.SnapshotRequest
	return event, nil
}

func (zkdao *ZkDao) UpdateSnapshotRequest(snapshotRequest *dao.SnapshotRequest) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return UpdateSnapshotRequest(conn, snapshotRequest)
}

func UpdateSnapshotRequest(conn coordclient.Connection, snapshotRequest *dao.SnapshotRequest) error {
	glog.V(3).Infof("UpdateSnapshotRequest with snapshotrequest: %+v", snapshotRequest)
	snapshotRequestsPath := SnapshotRequestsPath(snapshotRequest.Id)
	exists, err := conn.Exists(snapshotRequestsPath)
	if err != nil {
		if err == coordclient.ErrNoNode {
			return AddSnapshotRequest(conn, snapshotRequest)
		}
	}
	if !exists {
		return AddSnapshotRequest(conn, snapshotRequest)
	}

	srn := SnapShotRequestNode{}
	if err := conn.Get(snapshotRequestsPath, &srn); err != nil {
		return err
	}
	srn.SnapshotRequest = snapshotRequest
	glog.Infof("Setting snapshotrequest: %+v, %+v", srn, srn.SnapshotRequest)
	return conn.Set(snapshotRequestsPath, &srn)
}

func (zkdao *ZkDao) RemoveSnapshotRequest(requestID string) error {
	conn, err := zkdao.client.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	return RemoveSnapshotRequest(conn, requestID)
}

func RemoveSnapshotRequest(conn coordclient.Connection, requestID string) error {
	snapshotRequestsPath := SnapshotRequestsPath(requestID)
	if err := conn.Delete(snapshotRequestsPath); err != nil {
		glog.Errorf("Unable to delete SnapshotRequest znode:%s error:%v", snapshotRequestsPath, err)
		return err
	}

	return nil
}

// Snapshot section end
