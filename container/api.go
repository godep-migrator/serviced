package container

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	dockerclient "github.com/zenoss/go-dockerclient"
)

const (
	dockerep = "unix:///var/run/docker.sock"
	emptystr = ""
)

type (
	ContainerDefinition struct {
		ContainerOptions dockerclient.CreateContainerOptions
		HostConfig       dockerclient.HostConfig
	}

	handlerSet struct {
		sync.Mutex
		handlers []func(c *dockerclient.Container) error
	}
)

var (
	ErrRequestTimeout = errors.New("request timed out")

	dc dockerclient.Client
	em dockerclient.EventMonitor

	createHandlers = handlerSet{sync.Mutex{}, []func(c *dockerclient.Container) error{}}
	startHandlers  = handlerSet{sync.Mutex{}, []func(c *dockerclient.Container) error{}}
)

func init() {
	client, err := dockerclient.NewClient(dockerep)
	if err != nil {
		panic(fmt.Sprintf("can't create Docker client: %v", err))
	}
	dc = *client

	monitor, err := dc.MonitorEvents()
	if err != nil {
		panic(fmt.Sprintf("can't monitor Docker events: %v", err))
	}
	em = monitor
}

func StartContainer(cd *ContainerDefinition, timeout time.Duration) (string, error) {
	ctr, err := dc.CreateContainer(cd.ContainerOptions)
	switch {
	case err == dockerclient.ErrNoSuchImage:
		if pullerr := dc.PullImage(dockerclient.PullImageOptions{
			Repository:   cd.ContainerOptions.Config.Image,
			OutputStream: os.NewFile(uintptr(syscall.Stdout), "/dev/stdout"),
		}, dockerclient.AuthConfiguration{}); pullerr != nil {
			return emptystr, err
		}

		ctr, err = dc.CreateContainer(cd.ContainerOptions)
		if err != nil {
			return emptystr, nil
		}
	case err != nil:
		return emptystr, err
	}

	handleContainerCreation(ctr)

	s, err := em.Subscribe(ctr.ID)
	if err != nil {
		return emptystr, fmt.Errorf("can't subscribe to Docker events on container %s: %v", ctr.ID, err)
	}

	emc := make(chan struct{})

	s.Handle(dockerclient.Start, func(e dockerclient.Event) error {
		emc <- struct{}{}
		return nil
	})

	err = dc.StartContainer(ctr.ID, &cd.HostConfig)
	if err != nil {
		return emptystr, err
	} else {
		select {
		case <-time.After(timeout):
			return emptystr, ErrRequestTimeout
		case <-emc:
			handleContainerStart(ctr)
			return ctr.ID, nil
		}
	}
}

func StopContainer(id string, timeout time.Duration) error {
	return dc.StopContainer(id, uint(timeout))
}

func OnContainerCreate(handler func(c *dockerclient.Container) error) error {
	createHandlers.Lock()
	defer createHandlers.Unlock()

	createHandlers.handlers = append(createHandlers.handlers, handler)
	return nil
}

func OnContainerStart(handler func(c *dockerclient.Container) error) error {
	startHandlers.Lock()
	defer startHandlers.Unlock()

	startHandlers.handlers = append(startHandlers.handlers, handler)
	return nil
}

func handleContainerCreation(c *dockerclient.Container) {
	createHandlers.Lock()
	defer createHandlers.Unlock()

	for _, handler := range createHandlers.handlers {
		handler(c)
	}
}

func handleContainerStart(c *dockerclient.Container) {
	startHandlers.Lock()
	defer startHandlers.Unlock()

	for _, handler := range startHandlers.handlers {
		handler(c)
	}
}
