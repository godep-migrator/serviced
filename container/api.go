package container

import (
	"fmt"

	dockerclient "github.com/zenoss/go-dockerclient"
)

const (
	dockerep = "unix:///var/run/docker.sock"
	emptystr = ""
)

type (
	ContainerDefinition struct {
		dockerclient.CreateContainerOptions
		dockerclient.HostConfig
	}
)

var (
	ErrRequestTimeout = errors.New("request timed out")

	dc dockerclient.Client
)

func init() {
	dc, err := dockerclient.NewClient(dockerep)
	if err != nil {
		panic(fmt.Sprintf("can't create Docker client: %v", err))
	}
}

func StartContainer(cd ContainerDefinition, timeout Duration) (string, error) {
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

	err = dc.StartContainer(ctr.ID, cd.HostConfig)
	if err != nil {
		return emptystr, err
	} else {
		select {
		case <-time.After(timeout):
			return emptystr, ErrRequestTimeout
		case <-emc:
			return ctr.ID, nil
		}
	}
}

func StopContainer(id string, timeout Duration) error {
	return dc.StopContainer(id, uint(timeout))
}

func OnContainerStart(sf ContainerStartFunc) error {
	return nil
}

func handleContainerCreation(id string) []error {
	for _, handler := range containerCreationHandlers {
		if handler.applies(cd
}
<`0`>
