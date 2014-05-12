// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by a
// license that can be found in the LICENSE file.

// Package agent implements a service that runs on a serviced node. It is
// responsible for ensuring that a particular node is running the correct services
// and reporting the state and health of those services back to the master
// serviced.

package elasticsearch

import (
	dutils "github.com/dotcloud/docker/utils"
	"github.com/mattbaird/elastigo/api"
	"github.com/zenoss/glog"
	docker "github.com/zenoss/go-dockerclient"
	"github.com/zenoss/serviced/commons"
	coordclient "github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/datastore"
	"github.com/zenoss/serviced/dfs"
	"github.com/zenoss/serviced/domain/addressassignment"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicedefinition"
	"github.com/zenoss/serviced/domain/servicestate"
	"github.com/zenoss/serviced/domain/servicetemplate"
	"github.com/zenoss/serviced/facade"
	"github.com/zenoss/serviced/isvcs"
	"github.com/zenoss/serviced/volume"
	"github.com/zenoss/serviced/zzk"

	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	DOCKER_ENDPOINT string = "unix:///var/run/docker.sock"
)

//assert interface
var _ dao.ControlPlane = &ControlPlaneDao{}

type ControlPlaneDao struct {
	hostName string
	port     int
	varpath  string
	vfs      string
	zclient  *coordclient.Client
	zkDao    *zzk.ZkDao
	dfs      *dfs.DistributedFileSystem
	//needed while we move things over
	facade         *facade.Facade
	dockerRegistry string
}

func walkTree(node *treenode) []string {
	if len(node.children) == 0 {
		return []string{node.id}
	}
	relatedServiceIds := make([]string, 0)
	for _, childNode := range node.children {
		for _, childId := range walkTree(childNode) {
			relatedServiceIds = append(relatedServiceIds, childId)
		}
	}
	return append(relatedServiceIds, node.id)
}

type treenode struct {
	id       string
	parent   string
	children []*treenode
}

func (this *ControlPlaneDao) getServiceTree(serviceId string, servicesList *[]*service.Service) (servicesMap map[string]*treenode, topService *treenode) {
	glog.V(2).Infof(" getServiceTree = %s", serviceId)
	servicesMap = make(map[string]*treenode)
	for _, svc := range *servicesList {
		servicesMap[svc.Id] = &treenode{
			svc.Id,
			svc.ParentServiceId,
			[]*treenode{},
		}
	}

	// second time through builds our tree
	root := treenode{"root", "", []*treenode{}}
	for _, svc := range *servicesList {
		node := servicesMap[svc.Id]
		parent, found := servicesMap[svc.ParentServiceId]
		// no parent means this node belongs to root
		if !found {
			parent = &root
		}
		parent.children = append(parent.children, node)
	}

	// now walk up the tree, then back down capturing all siblings for this service ID
	topService = servicesMap[serviceId]
	for len(topService.parent) != 0 {
		topService = servicesMap[topService.parent]
	}
	return
}

// Get a service endpoint.
func (this *ControlPlaneDao) GetServiceEndpoints(serviceId string, response *map[string][]*dao.ApplicationEndpoint) (err error) {
	glog.V(2).Infof("ControlPlaneDao.GetServiceEndpoints serviceId=%s", serviceId)
	var myService service.Service
	err = this.GetService(serviceId, &myService)
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetServiceEndpoints service=%+v err=%s", myService, err)
		return
	}

	service_imports := myService.GetServiceImports()
	if len(service_imports) > 0 {
		glog.V(2).Infof("%+v service imports=%+v", myService, service_imports)

		var request dao.EntityRequest
		var servicesList []*service.Service
		err = this.GetServices(request, &servicesList)
		if err != nil {
			return
		}

		// Map all services by Id so we can construct a tree for the current service ID
		glog.V(2).Infof("ServicesList: %d", len(servicesList))
		_, topService := this.getServiceTree(serviceId, &servicesList)
		// We should now have the top-level service for the current service ID
		remoteEndpoints := make(map[string][]*dao.ApplicationEndpoint)

		//build 'OR' query to grab all service states with in "service" tree
		relatedServiceIds := walkTree(topService)
		var states []*servicestate.ServiceState
		err = this.zkDao.GetServiceStates(&states, relatedServiceIds...)
		if err != nil {
			return
		}

		// for each proxied port, find list of potential remote endpoints
		for _, endpoint := range service_imports {
			glog.V(2).Infof("Finding exports for import: %s %+v", endpoint.Application, endpoint)
			matchedEndpoint := false
			applicationRegex, err := regexp.Compile(fmt.Sprintf("^%s$", endpoint.Application))
			if err != nil {
				continue //Don't spam error message; it was reported at validation time
			}
			for _, ss := range states {
				hostPort, containerPort, protocol, match := ss.GetHostEndpointInfo(applicationRegex)
				if match {
					glog.V(1).Infof("Matched endpoint: %s.%s -> %s:%d (%s/%d)",
						myService.Name, endpoint.Application, ss.HostIp, hostPort, protocol, containerPort)
					// if port/protocol undefined in the import, use the export's values
					if endpoint.PortNumber != 0 {
						containerPort = endpoint.PortNumber
					}
					if endpoint.Protocol != "" {
						protocol = endpoint.Protocol
					}
					var ep dao.ApplicationEndpoint
					ep.ServiceId = ss.ServiceId
					ep.ContainerPort = containerPort
					ep.HostPort = hostPort
					ep.HostIp = ss.HostIp
					ep.ContainerIp = ss.PrivateIp
					ep.Protocol = protocol
					ep.VirtualAddress = endpoint.VirtualAddress

					key := fmt.Sprintf("%s:%d", protocol, containerPort)
					if _, exists := remoteEndpoints[key]; !exists {
						remoteEndpoints[key] = make([]*dao.ApplicationEndpoint, 0)
					}
					remoteEndpoints[key] = append(remoteEndpoints[key], &ep)
					matchedEndpoint = true
				}
			}
			if !matchedEndpoint {
				glog.V(1).Infof("Unmatched endpoint %s.%s", myService.Name, endpoint.Application)
			}
		}

		*response = remoteEndpoints
		glog.V(2).Infof("Return for %s is %+v", serviceId, remoteEndpoints)
	}
	return
}

// The tenant id is the root service uuid. Walk the service tree to root to find the tenant id.
func (this *ControlPlaneDao) GetTenantId(serviceId string, tenantId *string) (err error) {
	glog.V(2).Infof("ControlPlaneDao.GetTenantId: %s", serviceId)
	id := strings.TrimSpace(serviceId)
	if id == "" {
		return errors.New("empty serviceId not allowed")
	}

	var traverse func(string) (string, error)

	traverse = func(id string) (string, error) {
		var service service.Service
		if err := this.GetService(id, &service); err != nil {
			return "", err
		} else if service.ParentServiceId != "" {
			return traverse(service.ParentServiceId)
		} else {
			glog.V(1).Infof("parent service: %+v", service)
			return service.Id, nil
		}
	}

	*tenantId, err = traverse(id)
	return
}

//
func (this *ControlPlaneDao) AddService(svc service.Service, serviceId *string) error {
	glog.V(0).Infof("ControlPlaneDao.AddService: %+v", svc)
	store := service.NewStore()

	id := strings.TrimSpace(svc.Id)
	if id == "" {
		return errors.New("empty Service.Id not allowed")
	}
	svc.Id = id

	found := service.Service{}
	if err := store.Get(datastore.Get(), service.Key(svc.Id), &found); err != nil && !datastore.IsErrNoSuchEntity(err) {
		return err
	} else if err == nil {
		return fmt.Errorf("error adding service; %v already exists", id)
	}

	err := store.Put(datastore.Get(), service.Key(svc.Id), &svc)
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao.AddService: %+v", err)
		return err
	}
	*serviceId = id
	glog.V(0).Infof("ControlPlaneDao.AddService: id %+v; return id %v", id, serviceId)

	return this.zkDao.AddService(&svc)
}

// updateService internal method to use when service has been validated
func (this *ControlPlaneDao) updateService(svc *service.Service) error {
	id := strings.TrimSpace(svc.Id)
	if id == "" {
		return errors.New("empty Service.Id not allowed")
	}
	svc.Id = id
	//add assignment info to service
	for idx := range svc.Endpoints {
		assignment, err := this.getEndpointAddressAssignments(svc.Id, svc.Endpoints[idx].Name)
		if err != nil {
			glog.Errorf("ControlPlaneDao.UpdateService Error looking up address assignments: %v", err)
			return err
		}
		if assignment != nil {
			//assignment exists
			glog.V(4).Infof("ControlPlaneDao.UpdateService setting address assignment on endpoint: %s, %v", svc.Endpoints[idx].Name, assignment)
			svc.Endpoints[idx].SetAssignment(assignment)
		} else {
			svc.Endpoints[idx].RemoveAssignment()
		}
	}

	store := service.NewStore()
	ctx := datastore.Get()
	if err := store.Put(ctx, service.Key(id), svc); err != nil {
		return err
	}
	return this.zkDao.UpdateService(svc)
}

//
func (this *ControlPlaneDao) UpdateService(svc service.Service, unused *int) error {
	glog.V(2).Infof("ControlPlaneDao.UpdateService: %+v", svc)
	//cannot update service without validating it.
	if svc.DesiredState == service.SVCRun {
		if err := this.validateServicesForStarting(&svc, nil); err != nil {
			return err
		}

	}
	return this.updateService(&svc)
}

//
func (this *ControlPlaneDao) RemoveService(id string, unused *int) error {
	//TODO: should services already be stopped before removing to prevent half running service in case of error while deleting?

	err := this.walkServices(id, func(svc *service.Service) error {
		this.zkDao.RemoveService(svc.Id)
		return nil
	})

	if err != nil {
		//TODO: should we put them back?
		return err
	}

	store := service.NewStore()
	ctx := datastore.Get()

	err = this.walkServices(id, func(svc *service.Service) error {
		err := store.Delete(ctx, service.Key(svc.Id))
		if err != nil {
			glog.Errorf("Error removing service %s	 %s ", svc.Id, err)
		}
		return err
	})
	if err != nil {
		return err
	}
	//TODO: remove AddressAssignments with this Service
	return nil
}

//
func (this *ControlPlaneDao) GetService(id string, myService *service.Service) error {
	glog.V(3).Infof("ControlPlaneDao.GetService: id=%s", id)
	store := service.NewStore()
	request := service.Service{}
	err := store.Get(datastore.Get(), service.Key(id), &request)
	glog.V(3).Infof("ControlPlaneDao.GetService: id=%s, service=%+v, err=%s", id, request, err)
	*myService = request
	return err
}

func (this *ControlPlaneDao) GetRunningServices(request dao.EntityRequest, services *[]*dao.RunningService) error {
	return this.zkDao.GetAllRunningServices(services)
}

func (this *ControlPlaneDao) GetRunningServicesForHost(hostId string, services *[]*dao.RunningService) error {
	return this.zkDao.GetRunningServicesForHost(hostId, services)
}

func (this *ControlPlaneDao) GetRunningServicesForService(serviceId string, services *[]*dao.RunningService) error {
	return this.zkDao.GetRunningServicesForService(serviceId, services)
}

func (this *ControlPlaneDao) GetServiceLogs(id string, logs *string) error {
	glog.V(3).Info("ControlPlaneDao.GetServiceLogs id=", id)
	var serviceStates []*servicestate.ServiceState
	err := this.zkDao.GetServiceStates(&serviceStates, id)
	if err != nil {
		return err
	}
	if len(serviceStates) == 0 {
		glog.V(1).Info("Unable to find any running services for ", id)
		return nil
	}
	cmd := exec.Command("docker", "logs", serviceStates[0].DockerId)
	output, err := cmd.CombinedOutput()
	if err != nil {
		glog.Errorf("Unable to return logs because: %v", err)
		return err
	}
	*logs = string(output)
	return nil
}

func (this *ControlPlaneDao) GetServiceStateLogs(request dao.ServiceStateRequest, logs *string) error {
	/* TODO: This command does not support logs on other hosts */
	glog.V(3).Info("ControlPlaneDao.GetServiceStateLogs id=", request)
	var serviceState servicestate.ServiceState
	err := this.zkDao.GetServiceState(&serviceState, request.ServiceId, request.ServiceStateId)
	if err != nil {
		glog.Errorf("ControlPlaneDao.GetServiceStateLogs servicestate=%+v err=%s", serviceState, err)
		return err
	}

	cmd := exec.Command("docker", "logs", serviceState.DockerId)
	output, err := cmd.CombinedOutput()
	if err != nil {
		glog.Errorf("Unable to return logs because: %v", err)
		return err
	}
	*logs = string(output)
	return nil
}

//
func (this *ControlPlaneDao) GetServices(request dao.EntityRequest, services *[]*service.Service) error {
	glog.V(3).Infof("ControlPlaneDao.GetServices")
	store := service.NewStore()
	results, err := store.GetServices(datastore.Get())
	if err != nil {
		glog.Error("ControlPlaneDao.GetServices: err=", err)
		return err
	}
	*services = results
	return nil
}

//
func (this *ControlPlaneDao) GetTaggedServices(request dao.EntityRequest, services *[]*service.Service) error {
	glog.V(3).Infof("ControlPlaneDao.GetTaggedServices")

	store := service.NewStore()
	switch v := request.(type) {
	case []string:
		results, err := store.GetTaggedServices(datastore.Get(), v...)
		if err != nil {
			glog.Error("ControlPlaneDao.GetTaggedServices: err=", err)
			return err
		}
		*services = results
		glog.V(2).Infof("ControlPlaneDao.GetTaggedServices: services=%v", services)
		return nil
	default:
		err := fmt.Errorf("Bad request type: %v", v)
		glog.V(2).Info("ControlPlaneDao.GetTaggedServices: err=", err)
		return err
	}
}


// determine whether the services are ready for deployment
func (this *ControlPlaneDao) validateServicesForStarting(svc *service.Service, _ *struct{}) error {
	// ensure all endpoints with AddressConfig have assigned IPs
	for _, endpoint := range svc.Endpoints {
		needsAnAddressAssignment, addressAssignmentId, err := this.needsAddressAssignment(svc.Id, endpoint)
		if err != nil {
			return err
		}

		if needsAnAddressAssignment {
			msg := fmt.Sprintf("Service ID %s is in need of an AddressAssignment: %s", svc.Id, addressAssignmentId)
			return errors.New(msg)
		} else if addressAssignmentId != "" {
			glog.Infof("AddressAssignment: %s already exists", addressAssignmentId)
		}
	}

	if svc.RAMCommitment < 0 {
		return fmt.Errorf("service RAM commitment cannot be negative")
	}

	// add additional validation checks to the services
	return nil
}

// assign an IP address to a service (and all its child services) containing non default AddressResourceConfig
func (this *ControlPlaneDao) AssignIPs(assignmentRequest dao.AssignmentRequest, _ *struct{}) error {
	myService := service.Service{}
	err := this.GetService(assignmentRequest.ServiceId, &myService)
	if err != nil {
		return err
	}

	// populate poolsIpInfo
	poolIPs, err := this.facade.GetPoolIPs(datastore.Get(), myService.PoolId)
	if err != nil {
		glog.Errorf("GetPoolsIPInfo failed: %v", err)
		return err
	}
	poolsIpInfo := poolIPs.HostIPs
	if len(poolsIpInfo) < 1 {
		msg := fmt.Sprintf("No IP addresses are available in pool %s.", myService.PoolId)
		return errors.New(msg)
	}
	glog.Infof("Pool %v contains %v available IP(s)", myService.PoolId, len(poolsIpInfo))

	rand.Seed(time.Now().UTC().UnixNano())
	ipIndex := 0
	userProvidedIPAssignment := false

	if assignmentRequest.AutoAssignment {
		// automatic IP requested
		glog.Infof("Automatic IP Address Assignment")
		ipIndex = rand.Intn(len(poolsIpInfo))
	} else {
		// manual IP provided
		// verify that the user provided IP address is available in the pool
		glog.Infof("Manual IP Address Assignment")
		validIp := false
		userProvidedIPAssignment = true

		for index, hostIPResource := range poolsIpInfo {
			if assignmentRequest.IpAddress == hostIPResource.IPAddress {
				// WHAT HAPPENS IF THERE EXISTS THE SAME IP ON MORE THAN ONE HOST???
				validIp = true
				ipIndex = index
				break
			}
		}

		if !validIp {
			msg := fmt.Sprintf("The requested IP address: %s is not contained in pool %s.", assignmentRequest.IpAddress, myService.PoolId)
			return errors.New(msg)
		}
	}
	assignmentRequest.IpAddress = poolsIpInfo[ipIndex].IPAddress
	selectedHostId := poolsIpInfo[ipIndex].HostID
	glog.Infof("Attempting to set IP address(es) to %s", assignmentRequest.IpAddress)

	assignments := []*addressassignment.AddressAssignment{}
	this.GetServiceAddressAssignments(assignmentRequest.ServiceId, &assignments)
	if err != nil {
		glog.Errorf("controlPlaneDao.GetServiceAddressAssignments failed in anonymous function: %v", err)
		return err
	}

	visitor := func(myService *service.Service) error {
		// if this service is in need of an IP address, assign it an IP address
		for _, endpoint := range myService.Endpoints {
			needsAnAddressAssignment, addressAssignmentId, err := this.needsAddressAssignment(myService.Id, endpoint)
			if err != nil {
				return err
			}

			// if an address assignment is needed (does not yet exist) OR
			// if a specific IP address is provided by the user AND an address assignment already exists
			if needsAnAddressAssignment || (userProvidedIPAssignment && addressAssignmentId != "") {
				if addressAssignmentId != "" {
					glog.Infof("Removing AddressAssignment: %s", addressAssignmentId)
					err = this.RemoveAddressAssignment(addressAssignmentId, nil)
					if err != nil {
						glog.Errorf("controlPlaneDao.RemoveAddressAssignment failed in AssignIPs anonymous function: %v", err)
						return err
					}
				}
				assignment := addressassignment.AddressAssignment{}
				assignment.AssignmentType = "static"
				assignment.HostID = selectedHostId
				assignment.PoolID = myService.PoolId
				assignment.IPAddr = assignmentRequest.IpAddress
				assignment.Port = endpoint.AddressConfig.Port
				assignment.ServiceID = myService.Id
				assignment.EndpointName = endpoint.Name
				glog.Infof("Creating AddressAssignment for Endpoint: %s", assignment.EndpointName)

				var unusedStr string
				err = this.AssignAddress(assignment, &unusedStr)
				if err != nil {
					glog.Errorf("AssignAddress failed in AssignIPs anonymous function: %v", err)
					return err
				}

				err = this.updateService(myService)
				if err != nil {
					glog.Errorf("Failed to update service w/AssignAddressAssignment: %v", err)
					return err
				}

				glog.Infof("Created AddressAssignment: %s for Endpoint: %s", assignment.ID, assignment.EndpointName)
			}
		}
		return nil
	}

	// traverse all the services
	err = this.walkServices(assignmentRequest.ServiceId, visitor)
	if err != nil {
		return err
	}

	glog.Infof("All services requiring an explicit IP address (at this moment) from service: %v and down ... have been assigned: %s", assignmentRequest.ServiceId, assignmentRequest.IpAddress)
	return nil
}

// validate the provided service
func (this *ControlPlaneDao) validateService(serviceId string) error {
	//TODO: create map of IPs to ports and ensure that an IP does not have > 1 process listening on the same port
	visitor := func(service *service.Service) error {
		// validate the service is ready to start
		err := this.validateServicesForStarting(service, nil)
		if err != nil {
			glog.Errorf("Services failed validation for starting")
			return err
		}
		return nil
	}

	// traverse all the services
	return this.walkServices(serviceId, visitor)
}

// start the provided service
func (this *ControlPlaneDao) StartService(serviceId string, unused *string) error {
	// this will traverse all the services
	err := this.validateService(serviceId)
	if err != nil {
		return err
	}

	visitor := func(svc *service.Service) error {
		//start this service
		svc.DesiredState = service.SVCRun
		err = this.updateService(svc)
		if err != nil {
			return err
		}
		return nil
	}

	// traverse all the services
	return this.walkServices(serviceId, visitor)
}

// traverse all the services (including the children of the provided service)
func (this *ControlPlaneDao) walkServices(serviceID string, visitFn service.Visit) error {

	store := service.NewStore()
	ctx := datastore.Get()

	getChildren := func(parentID string) ([]*service.Service, error) {
		return store.GetChildServices(ctx, parentID)
	}
	getService := func(svcID string) (service.Service, error) {
		svc := service.Service{}
		err := store.Get(ctx, service.Key(svcID), &svc)
		return svc, err
	}

	return service.Walk(serviceID, visitFn, getService, getChildren)
}

func (this *ControlPlaneDao) GetServiceState(request dao.ServiceStateRequest, serviceState *servicestate.ServiceState) error {
	glog.V(3).Infof("ControlPlaneDao.GetServiceState: request=%v", request)
	return this.zkDao.GetServiceState(serviceState, request.ServiceId, request.ServiceStateId)
}

func (this *ControlPlaneDao) GetRunningService(request dao.ServiceStateRequest, running *dao.RunningService) error {
	glog.V(3).Infof("ControlPlaneDao.GetRunningService: request=%v", request)
	return this.zkDao.GetRunningService(request.ServiceId, request.ServiceStateId, running)
}

func (this *ControlPlaneDao) GetServiceStates(serviceId string, serviceStates *[]*servicestate.ServiceState) error {
	glog.V(2).Infof("ControlPlaneDao.GetServiceStates: serviceId=%s", serviceId)
	return this.zkDao.GetServiceStates(serviceStates, serviceId)
}

/* This method assumes that if a service instance exists, it has not yet been terminated */
func (this *ControlPlaneDao) getNonTerminatedServiceStates(serviceId string, serviceStates *[]*servicestate.ServiceState) error {
	glog.V(2).Infof("ControlPlaneDao.getNonTerminatedServiceStates: serviceId=%s", serviceId)
	return this.zkDao.GetServiceStates(serviceStates, serviceId)
}

// Update the current state of a service instance.
func (this *ControlPlaneDao) UpdateServiceState(state servicestate.ServiceState, unused *int) error {
	glog.V(2).Infoln("ControlPlaneDao.UpdateServiceState state=%+v", state)
	return this.zkDao.UpdateServiceState(&state)
}

func (this *ControlPlaneDao) RestartService(serviceId string, unused *int) error {
	return dao.ControlPlaneError{"Unimplemented"}
}

func (this *ControlPlaneDao) StopService(id string, unused *int) error {
	glog.V(0).Info("ControlPlaneDao.StopService id=", id)

	visitor := func(svc *service.Service) error {
		//start this service
		if svc.Launch == commons.MANUAL {
			return nil
		}
		svc.DesiredState = service.SVCStop
		if err := this.updateService(svc); err != nil {
			return err
		}
		return nil
	}

	// traverse all the services
	return this.walkServices(id, visitor)
}

func (this *ControlPlaneDao) StopRunningInstance(request dao.HostServiceRequest, unused *int) error {
	return this.zkDao.TerminateHostService(request.HostId, request.ServiceStateId)
}

func (this *ControlPlaneDao) DeployTemplate(request dao.ServiceTemplateDeploymentRequest, tenantId *string) error {
	store := servicetemplate.NewStore()
	template, err := store.Get(datastore.Get(), request.TemplateId)
	if err != nil {
		glog.Errorf("unable to load template: %s", request.TemplateId)
		return err
	}

	pool, err := this.facade.GetResourcePool(datastore.Get(), request.PoolId)
	if err != nil {
		glog.Errorf("Unable to load resource pool: %s", request.PoolId)
		return err
	}
	if pool == nil {
		return fmt.Errorf("poolid %s not found", request.PoolId)
	}

	volumes := make(map[string]string)
	return this.deployServiceDefinitions(template.Services, request.TemplateId, request.PoolId, "", volumes, request.DeploymentId, tenantId)
}

func getSubServiceImageIds(ids map[string]struct{}, svc servicedefinition.ServiceDefinition) {
	found := struct{}{}

	if len(svc.ImageID) != 0 {
		ids[svc.ImageID] = found
	}
	for _, s := range svc.Services {
		getSubServiceImageIds(ids, s)
	}
}

func (this *ControlPlaneDao) deployServiceDefinitions(sds []servicedefinition.ServiceDefinition, template string, pool string, parentServiceId string, volumes map[string]string, deploymentId string, tenantId *string) error {
	// ensure that all images in the templates exist
	imageIds := make(map[string]struct{})
	for _, svc := range sds {
		getSubServiceImageIds(imageIds, svc)
	}

	dockerclient, err := docker.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		glog.Errorf("unable to start docker client")
		return err
	}

	for imageId, _ := range imageIds {

		image, err := dockerclient.InspectImage(imageId)
		if err != nil {
			glog.Errorf("could not look up image: %s", imageId)
			return err
		}

		repo, err := this.renameImageId(imageId, *tenantId)
		if err != nil {
			glog.Errorf("malformed imageId: %s", imageId)
			return err
		}

		options := docker.TagImageOptions{
			Repo:  repo,
			Force: true,
		}
		if err := dockerclient.TagImage(image.ID, options); err != nil {
			glog.Errorf("could not tag image: %s options: %+v", image.ID, options)
			return err
		}
		// TODO: push image to local registry
	}

	for _, sd := range sds {
		if err := this.deployServiceDefinition(sd, template, pool, parentServiceId, volumes, deploymentId, tenantId); err != nil {
			return err
		}
	}
	return nil
}

func (this *ControlPlaneDao) renameImageId(imageId, tenantId string) (string, error) {

	repo, _ := dutils.ParseRepositoryTag(imageId)
	re := regexp.MustCompile("/?([^/]+)\\z")
	matches := re.FindStringSubmatch(repo)
	if matches == nil {
		return "", errors.New("malformed imageid")
	}
	name := matches[1]

	return fmt.Sprintf("%s/%s_%s", this.dockerRegistry, tenantId, name), nil
}

func (this *ControlPlaneDao) deployServiceDefinition(sd servicedefinition.ServiceDefinition, template string, pool string, parentServiceId string, volumes map[string]string, deploymentId string, tenantId *string) error {
	// Always deploy in stopped state, starting is a separate step
	ds := service.SVCStop

	exportedVolumes := make(map[string]string)
	for k, v := range volumes {
		exportedVolumes[k] = v
	}
	svc, err := service.BuildService(sd, parentServiceId, pool, ds, deploymentId)
	if err != nil {
		return err
	}

	getSvc := func(svcID string) (service.Service, error) {
		svc := service.Service{}
		err := this.GetService(svcID, &svc)
		return svc, err
	}

	//for each endpoint, evaluate it's Application
	if err = svc.EvaluateEndpointTemplates(getSvc); err != nil {
		return err
	}

	//for each endpoint, evaluate it's Application
	if err = svc.EvaluateEndpointTemplates(getSvc); err != nil {
		return err
	}

	if parentServiceId == "" {
		*tenantId = svc.Id
	}

	// Using the tenant id, tag the base image with the tenantID
	if svc.ImageId != "" {
		name, err := this.renameImageId(svc.ImageId, *tenantId)
		if err != nil {
			return err
		}
		svc.ImageId = name
	}

	var serviceId string
	err = this.AddService(*svc, &serviceId)
	if err != nil {
		return err
	}

	return this.deployServiceDefinitions(sd.Services, template, pool, svc.Id, exportedVolumes, deploymentId, tenantId)
}

func (this *ControlPlaneDao) AddServiceTemplate(serviceTemplate servicetemplate.ServiceTemplate, templateId *string) error {
	uuid, err := dao.NewUuid()
	if err != nil {
		return err
	}
	serviceTemplate.ID = uuid

	store := servicetemplate.NewStore()
	if err = store.Put(datastore.Get(), serviceTemplate); err != nil {
		return err
	}

	*templateId = uuid
	// this takes a while so don't block the main thread
	go this.reloadLogstashContainer()
	return err
}

func (this *ControlPlaneDao) UpdateServiceTemplate(template servicetemplate.ServiceTemplate, unused *int) error {
	store := servicetemplate.NewStore()
	if err := store.Put(datastore.Get(), template); err != nil {
		return err
	}
	go this.reloadLogstashContainer() // don't block the main thread
	return nil
}

func (this *ControlPlaneDao) RemoveServiceTemplate(id string, unused *int) error {
	// make sure it is a valid template first
	store := servicetemplate.NewStore()

	_, err := store.Get(datastore.Get(), id)
	if err != nil {
		return fmt.Errorf("Unable to find template: %s", id)
	}

	glog.V(2).Infof("ControlPlaneDao.RemoveServiceTemplate: %s", id)
	if err != store.Delete(datastore.Get(), id) {
		return err
	}
	go this.reloadLogstashContainer()
	return nil
}



func (this *ControlPlaneDao) GetServiceTemplates(unused int, templates *map[string]*servicetemplate.ServiceTemplate) error {
	glog.V(2).Infof("ControlPlaneDao.GetServiceTemplates")
	store := servicetemplate.NewStore()
	results, err := store.GetServiceTemplates(datastore.Get())
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetServiceTemplates: err=%s", err)
		return err
	}
	templatemap := make(map[string]*servicetemplate.ServiceTemplate)
	for _, st := range results {
		templatemap[st.ID] = st
	}
	*templates = templatemap
	return nil
}

func (this *ControlPlaneDao) StartShell(service service.Service, unused *int) error {
	// TODO: implement stub
	return nil
}

func (this *ControlPlaneDao) ExecuteShell(service service.Service, command *string) error {
	// TODO: implement stub
	return nil
}

func (this *ControlPlaneDao) ShowCommands(service service.Service, unused *int) error {
	// TODO: implement stub
	return nil
}

func (this *ControlPlaneDao) DeleteSnapshot(snapshotId string, unused *int) error {
	return this.dfs.DeleteSnapshot(snapshotId)
}

func (this *ControlPlaneDao) DeleteSnapshots(serviceId string, unused *int) error {
	var tenantId string
	if err := this.GetTenantId(serviceId, &tenantId); err != nil {
		glog.V(2).Infof("ControlPlaneDao.DeleteSnapshots err=%s", err)
		return err
	}

	if serviceId != tenantId {
		glog.Infof("ControlPlaneDao.DeleteSnapshots service is not the parent, service=%s, tenant=%s", serviceId, tenantId)
		return nil
	}

	return this.dfs.DeleteSnapshots(tenantId)
}

func (this *ControlPlaneDao) Rollback(snapshotId string, unused *int) error {
	return this.dfs.Rollback(snapshotId)
}

// Takes a snapshot of the DFS via the host
func (this *ControlPlaneDao) LocalSnapshot(serviceId string, label *string) error {
	var tenantId string
	if err := this.GetTenantId(serviceId, &tenantId); err != nil {
		glog.Errorf("ControlPlaneDao.LocalSnapshot err=%s", err)
		return err
	}

	if id, err := this.dfs.Snapshot(tenantId); err != nil {
		glog.Errorf("ControlPlaneDao.LocalSnapshot err=%s", err)
		return err
	} else {
		*label = id
	}

	return nil
}

// Snapshot is called via RPC by the CLI to take a snapshot for a serviceId
func (this *ControlPlaneDao) Snapshot(serviceId string, label *string) error {
	glog.V(3).Infof("ControlPlaneDao.Snapshot entering snapshot with service=%s", serviceId)
	defer glog.V(3).Infof("ControlPlaneDao.Snapshot finished snapshot for service=%s", serviceId)

	var tenantId string
	if err := this.GetTenantId(serviceId, &tenantId); err != nil {
		glog.V(2).Infof("ControlPlaneDao: dao.LocalSnapshot err=%s", err)
		return err
	}

	// request a snapshot by placing request znode in zookeeper - leader will notice
	snapshotRequest, err := dao.NewSnapshotRequest(serviceId, "")
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao: dao.NewSnapshotRequest err=%s", err)
		return err
	}
	if err := this.zkDao.AddSnapshotRequest(snapshotRequest); err != nil {
		glog.V(2).Infof("ControlPlaneDao.zkDao.AddSnapshotRequest err=%s", err)
		return err
	}
	// TODO:
	//	requestId := snapshotRequest.Id
	//	defer this.zkDao.RemoveSnapshotRequest(requestId)

	glog.Infof("added snapshot request: %+v", snapshotRequest)

	// wait for completion of snapshot request - check only once a second
	// BEWARE: this.zkDao.LoadSnapshotRequestW does not block like it should
	//         thus cannot use idiomatic select on eventChan and time.After() channels
	timeOutValue := time.Second * 60
	endTime := time.Now().Add(timeOutValue)
	for time.Now().Before(endTime) {
		glog.V(2).Infof("watching for snapshot completion for request: %+v", snapshotRequest)
		_, err := this.zkDao.LoadSnapshotRequestW(snapshotRequest.Id, snapshotRequest)
		switch {
		case err != nil:
			glog.Infof("failed snapshot request: %+v  error: %s", snapshotRequest, err)
			return err
		case snapshotRequest.SnapshotError != "":
			glog.Infof("failed snapshot request: %+v  error: %s", snapshotRequest, snapshotRequest.SnapshotError)
			return errors.New(snapshotRequest.SnapshotError)
		case snapshotRequest.SnapshotLabel != "":
			*label = snapshotRequest.SnapshotLabel
			glog.Infof("completed snapshot request: %+v  label: %s", snapshotRequest, *label)
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	err = fmt.Errorf("timed out waiting %v for snapshot: %+v", timeOutValue, snapshotRequest)
	glog.Error(err)
	return err
}

func (this *ControlPlaneDao) GetVolume(serviceId string, theVolume *volume.Volume) error {
	var tenantId string
	if err := this.GetTenantId(serviceId, &tenantId); err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetVolume service=%+v err=%s", serviceId, err)
		return err
	}
	glog.V(3).Infof("ControlPlaneDao.GetVolume service=%+v tenantId=%s", serviceId, tenantId)
	var service service.Service
	if err := this.GetService(tenantId, &service); err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetVolume service=%+v err=%s", serviceId, err)
		return err
	}
	glog.V(3).Infof("ControlPlaneDao.GetVolume service=%+v poolId=%s", service, service.PoolId)

	aVolume, err := getSubvolume(this.vfs, service.PoolId, tenantId)
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetVolume service=%+v err=%s", serviceId, err)
		return err
	}
	if aVolume == nil {
		glog.V(2).Infof("ControlPlaneDao.GetVolume service=%+v volume=nil", serviceId)
		return errors.New("volume is nil")
	}

	glog.V(2).Infof("ControlPlaneDao.GetVolume service=%+v volume2=%+v %v", serviceId, aVolume, aVolume)
	*theVolume = *aVolume
	return nil
}

// Commits a container to an image and saves it on the DFS
func (this *ControlPlaneDao) Commit(containerId string, label *string) error {
	if id, err := this.dfs.Commit(containerId); err != nil {
		glog.V(2).Infof("ControlPlaneDao.GetVolume containerId=%s err=%s", containerId, err)
		return err
	} else {
		*label = id
	}

	return nil
}

func getSubvolume(vfs, poolId, tenantId string) (*volume.Volume, error) {
	baseDir, err := filepath.Abs(path.Join(varPath(), "volumes", poolId))
	if err != nil {
		return nil, err
	}
	glog.Infof("Mounting vfs:%v tenantId:%v baseDir:%v\n", vfs, tenantId, baseDir)
	return volume.Mount(vfs, tenantId, baseDir)
}

func varPath() string {
	if len(os.Getenv("SERVICED_HOME")) > 0 {
		return path.Join(os.Getenv("SERVICED_HOME"), "var")
	} else if user, err := user.Current(); err == nil {
		return path.Join(os.TempDir(), "serviced-"+user.Username, "var")
	} else {
		defaultPath := "/tmp/serviced/var"
		glog.Warningf("Defaulting varPath to:%v\n", defaultPath)
		return defaultPath
	}
}

func (this *ControlPlaneDao) Snapshots(serviceId string, labels *[]string) error {

	var tenantId string
	if err := this.GetTenantId(serviceId, &tenantId); err != nil {
		glog.V(2).Infof("ControlPlaneDao.Snapshots service=%+v err=%s", serviceId, err)
		return err
	}
	var service service.Service
	err := this.GetService(tenantId, &service)
	if err != nil {
		glog.V(2).Infof("ControlPlaneDao.Snapshots service=%+v err=%s", serviceId, err)
		return err
	}

	if volume, err := getSubvolume(this.vfs, service.PoolId, tenantId); err != nil {
		glog.V(2).Infof("ControlPlaneDao.Snapshots service=%+v err=%s", serviceId, err)
		return err
	} else {
		if snaplabels, err := volume.Snapshots(); err != nil {
			return err
		} else {
			glog.Infof("Got snap labels %v", snaplabels)
			*labels = snaplabels
		}
	}
	return nil
}

func (this *ControlPlaneDao) Get(service service.Service, file *string) error {
	// TODO: implement stub
	return nil
}

func (this *ControlPlaneDao) Send(service service.Service, files *[]string) error {
	// TODO: implment stub
	return nil
}

// Create a elastic search control plane data access object
func NewControlPlaneDao(hostName string, port int, facade *facade.Facade, dockerRegistry string) (*ControlPlaneDao, error) {
	glog.V(0).Infof("Opening ElasticSearch ControlPlane Dao: hostName=%s, port=%d", hostName, port)
	api.Domain = hostName
	api.Port = strconv.Itoa(port)

	dao := &ControlPlaneDao{
		hostName:       hostName,
		port:           port,
		dockerRegistry: dockerRegistry,
	}
	if dfs, err := dfs.NewDistributedFileSystem(dao, facade); err != nil {
		return nil, err
	} else {
		dao.dfs = dfs
	}

	return dao, nil
}

func NewControlSvc(hostName string, port int, facade *facade.Facade, zclient *coordclient.Client, varpath, vfs string, dockerRegistry string) (*ControlPlaneDao, error) {
	glog.V(2).Info("calling NewControlSvc()")
	defer glog.V(2).Info("leaving NewControlSvc()")

	s, err := NewControlPlaneDao(hostName, port, facade, dockerRegistry)
	if err != nil {
		return nil, err
	}

	//Used to bridge old to new
	s.facade = facade

	s.varpath = varpath
	s.vfs = vfs

	s.zclient = zclient
	s.zkDao = zzk.NewZkDao(zclient)

	// create the account credentials
	if err = createSystemUser(s); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *ControlPlaneDao) ReadyDFS(unused bool, unusedint *int) (err error) {
	s.dfs.Lock()
	s.dfs.Unlock()
	return
}

// writeLogstashConfiguration takes all the available
// services and writes out the filters section for logstash.
// This is required before logstash startsup
func (s *ControlPlaneDao) writeLogstashConfiguration() error {
	var templatesMap map[string]*servicetemplate.ServiceTemplate
	if err := s.GetServiceTemplates(0, &templatesMap); err != nil {
		return err
	}

	// FIXME: eventually this file should live in the DFS or the config should
	// live in zookeeper to allow the agents to get to this
	if err := dao.WriteConfigurationFile(templatesMap); err != nil {
		return err
	}
	return nil
}

// Anytime the available service definitions are modified
// we need to restart the logstash container so it can write out
// its new filter set.
// This method depends on the elasticsearch container being up and running.
func (s *ControlPlaneDao) reloadLogstashContainer() error {
	err := s.writeLogstashConfiguration()
	if err != nil {
		glog.Fatalf("Could not write logstash configuration: %s", err)
		return err
	}
	glog.V(2).Info("Starting logstash container")
	if err := isvcs.Mgr.Notify("restart logstash"); err != nil {
		glog.Fatalf("Could not start logstash container: %s", err)
		return err
	}
	return nil
}
