package container

import (
	"github.com/zenoss/glog"
	"github.com/zenoss/serviced"
	"github.com/zenoss/serviced/commons/subprocess"
	"github.com/zenoss/serviced/dao"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/domain/servicedefinition"

	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	// ErrInvalidCommand is returned if a command is empty or malformed
	ErrInvalidCommand = errors.New("container: invalid command")
	// ErrInvalidEndpoint is returned if an endpoint is empty or malformed
	ErrInvalidEndpoint = errors.New("container: invalid endpoint")
	// ErrInvalidTenantID is returned if a TenantID is empty or malformed
	ErrInvalidTenantID = errors.New("container: invalid tenant id")
	// ErrInvalidServicedID is returned if a ServiceID is empty or malformed
	ErrInvalidServicedID = errors.New("container: invalid serviced id")
	// ErrInvalidServiced is returned if a Service is empty or malformed
	ErrInvalidService = errors.New("container: invalid serviced")
)

// ControllerOptions are options to be run when starting a new proxy server
type ControllerOptions struct {
	ServicedEndpoint string
	Service          struct {
		ID          string   // The uuid of the service to launch
		TenantID    string   // The tentant ID of the service
		Autorestart bool     // Controller will restart the service if it exits
		Command     []string // The command to launch
	}
	Mux struct { // TCPMUX configuration: RFC 1078
		Enabled     bool   // True if muxing is used
		Port        int    // the TCP port to use
		TLS         bool   // True if TLS is used
		KeyPEMFile  string // Path to the key file when TLS is used
		CertPEMFile string // Path to the cert file when TLS is used
	}
	Logforwarder struct { // Logforwarder configuration
		Enabled    bool   // True if enabled
		Path       string // Path to the logforwarder program
		ConfigFile string // Path to the config file for logstash-forwarder
	}
	Metric struct {
		Address       string // TCP port to host the metric service, :22350
		RemoteEndoint string // The url to forward metric queries
	}
}

// Controller is a object to manage the operations withing a container. For example,
// it creates the managed service instance, logstash forwarding, port forwarding, etc.
type Controller struct {
	options            ControllerOptions
	metricForwarder    *MetricForwarder
	logforwarder       *subprocess.Instance
	logforwarderExited chan error
	closing            chan chan error
}

// Close shuts down the controller
func (c *Controller) Close() error {
	errc := make(chan error)
	c.closing <- errc
	return <-errc
}

// getService retrieves a service
func getService(lbClientPort string, serviceID string) (*service.Service, error) {
	client, err := serviced.NewLBClient(lbClientPort)
	if err != nil {
		glog.Errorf("Could not create a client to endpoint: %s, %s", lbClientPort, err)
		return nil, err
	}
	defer client.Close()

	var service service.Service
	err = client.GetService(serviceID, &service)
	if err != nil {
		glog.Errorf("Error getting service %s  error: %s", serviceID, err)
		return nil, err
	}

	glog.V(1).Infof("getService: service: %+v", service)
	return &service, nil
}

// chownConfFile sets the owner and permissions for a file
func chownConfFile(filename, owner, permissions string) error {

	runCommand := func(exe, arg, filename string) error {
		command := exec.Command(exe, arg, filename)
		output, err := command.CombinedOutput()
		if err != nil {
			glog.Errorf("Error running command:'%v' output: %s  error: %s\n", command, output, err)
			return err
		}
		glog.Infof("Successfully ran command:'%v' output: %s\n", command, output)
		return nil
	}

	if owner != "" {
		if err := runCommand("chown", owner, filename); err != nil {
			return err
		}
	}
	if permissions != "" {
		if err := runCommand("chmod", permissions, filename); err != nil {
			return err
		}
	}

	return nil
}

// writeConfFile writes a config file
func writeConfFile(config servicedefinition.ConfigFile) error {
	// write file with default perms
	if err := ioutil.WriteFile(config.Filename, []byte(config.Content), os.FileMode(0664)); err != nil {
		glog.Errorf("Could not write out config file %s", config.Filename)
		return err
	}
	glog.Infof("Wrote config file %s", config.Filename)

	// change owner and permissions
	if err := chownConfFile(config.Filename, config.Owner, config.Permissions); err != nil {
		return err
	}

	return nil
}

// setupConfigFiles sets up config files
func setupConfigFiles(service *service.Service) error {
	// write out config files
	for _, config := range service.ConfigFiles {
		err := writeConfFile(config)
		if err != nil {
			return err
		}
	}
	return nil
}

// setupLogstashFiles sets up logstash files
func setupLogstashFiles(service *service.Service, resourcePath string) error {
	// write out logstash files
	if len(service.LogConfigs) != 0 {
		err := writeLogstashAgentConfig(LOGSTASH_CONTAINER_CONFIG, service, resourcePath)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewController creates a new Controller for the given options
func NewController(options ControllerOptions) (*Controller, error) {
	c := &Controller{
		options: options,
	}
	c.closing = make(chan chan error)

	if len(options.ServicedEndpoint) <= 0 {
		return nil, ErrInvalidEndpoint
	}

	// create config files
	service, err := getService(options.ServicedEndpoint, options.Service.ID)
	if err != nil {
		glog.Errorf("Invalid service from serviceID:%s", options.Service.ID)
		return c, ErrInvalidService
	}

	if err := setupConfigFiles(service); err != nil {
		glog.Errorf("Could not setup config files error:%s", err)
		return c, fmt.Errorf("container: invalid ConfigFiles error:%s", err)
	}

	if options.Logforwarder.Enabled {
		if err := setupLogstashFiles(service, filepath.Dir(options.Logforwarder.Path)); err != nil {
			glog.Errorf("Could not setup logstash files error:%s", err)
			return c, fmt.Errorf("container: invalid LogStashFiles error:%s", err)
		}

		// make sure we pick up any logfile that was modified within the
		// last three years
		// TODO: Either expose the 3 years a configurable or get rid of it
		logforwarder, exited, err := subprocess.New(time.Second,
			options.Logforwarder.Path,
			"-old-files-hours=26280",
			"-config", options.Logforwarder.ConfigFile)
		if err != nil {
			return nil, err
		}
		c.logforwarder = logforwarder
		c.logforwarderExited = exited
	}

	//build metric redirect url -- assumes 8444 is port mapped
	metricRedirect := options.Metric.RemoteEndoint
	if len(metricRedirect) == 0 {
		glog.V(1).Infof("container.Controller does not have metric forwarding")
	} else {
		if len(options.Service.TenantID) == 0 {
			return nil, ErrInvalidTenantID
		}
		if len(options.Service.ID) > 0 {
			return nil, ErrInvalidServicedID
		}
		metricRedirect += "&controlplane_service_id=" + options.Service.ID
		metricRedirect += "?controlplane_tenant_id=" + options.Service.TenantID
		//build and serve the container metric forwarder
		forwarder, err := NewMetricForwarder(options.Metric.Address, metricRedirect)
		if err != nil {
			return c, err
		}
		c.metricForwarder = forwarder
	}

	glog.Infof("command: %v [%d]", options.Service.Command, len(options.Service.Command))
	if len(options.Service.Command) < 1 {
		glog.Errorf("Invalid commandif ")
		return c, ErrInvalidCommand
	}

	return c, nil
}

// Run executes the controller's main loop and block until the service exits
// according to it's restart policy or Close() is called.
func (c *Controller) Run() (err error) {

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	args := []string{"-c", "exec " + strings.Join(c.options.Service.Command, " ")}

	service, serviceExited, _ := subprocess.New(time.Second*10, "/bin/sh", args...)

	var restartAfter <-chan time.Time
	for {
		select {
		case sig := <-sigc:
			switch sig {
			case syscall.SIGTERM:
				c.options.Service.Autorestart = false
			case syscall.SIGQUIT:
				c.options.Service.Autorestart = false
			case syscall.SIGINT:
				c.options.Service.Autorestart = false
			}
			glog.Infof("notifying subprocess of signal %v", sig)
			service.Notify(sig)
			select {
			case <-serviceExited:
				return
			default:
			}

		case <-time.After(time.Second * 10):
			c.handleRemotePorts()

		case <-serviceExited:
			if !c.options.Service.Autorestart {
				return
			}
			restartAfter = time.After(time.Second * 10)

		case <-restartAfter:
			if !c.options.Service.Autorestart {
				return
			}
			glog.Infof("restarting service process")
			service, serviceExited, _ = subprocess.New(time.Second*10, c.options.Service.Command[0], args...)
			restartAfter = nil

		}
	}
}

func (c *Controller) handleRemotePorts() {
	client, err := serviced.NewLBClient(c.options.ServicedEndpoint)
	if err != nil {
		glog.Errorf("Could not create a client to endpoint: %s, %s", c.options.ServicedEndpoint, err)
		return
	}
	defer client.Close()

	var endpoints map[string][]*dao.ApplicationEndpoint
	err = client.GetServiceEndpoints(c.options.Service.ID, &endpoints)
	if err != nil {
		glog.Errorf("Error getting application endpoints for service %s: %s", c.options.Service.ID, err)
		return
	}

	for key, endpointList := range endpoints {
		if len(endpointList) <= 0 {
			if proxy, ok := proxies[key]; ok {
				emptyAddressList := make([]string, 0)
				proxy.SetNewAddresses(emptyAddressList)
			}
			continue
		}

		addresses := make([]string, len(endpointList))
		for i, endpoint := range endpointList {
			glog.Infof("endpoints: %s, %v", key, *endpoint)
			addresses[i] = fmt.Sprintf("%s:%d", endpoint.HostIp, endpoint.HostPort)
		}
		sort.Strings(addresses)

		var (
			proxy *serviced.Proxy
			ok    bool
		)

		if proxy, ok = proxies[key]; !ok {
			glog.Infof("Attempting port map for: %s -> %+v", key, *endpointList[0])

			// setup a new proxy
			listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", endpointList[0].ContainerPort))
			if err != nil {
				glog.Errorf("Could not bind to port: %s", err)
				continue
			}
			proxy, err = serviced.NewProxy(
				fmt.Sprintf("%v", endpointList[0]),
				uint16(c.options.Mux.Port),
				c.options.Mux.TLS,
				listener)
			if err != nil {
				glog.Errorf("Could not build proxy %s", err)
				continue
			}

			glog.Infof("Success binding port: %s -> %+v", key, proxy)
			proxies[key] = proxy

			if ep := endpointList[0]; ep.VirtualAddress != "" {
				p := strconv.FormatUint(uint64(ep.ContainerPort), 10)
				err := vifs.RegisterVirtualAddress(ep.VirtualAddress, p, ep.Protocol)
				if err != nil {
					glog.Errorf("Error creating virtual address: %+v", err)
				}
			}
		}
		proxy.SetNewAddresses(addresses)
	}

}

var (
	proxies map[string]*serviced.Proxy
	vifs    *VIFRegistry
	nextip  int
)

func init() {
	proxies = make(map[string]*serviced.Proxy)
	vifs = NewVIFRegistry()
	nextip = 1
}
