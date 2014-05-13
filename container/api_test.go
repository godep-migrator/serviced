package container

import (
	"testing"
	"time"

	dockerclient "github.com/zenoss/go-dockerclient"
)

func TestOnStartContainer(t *testing.T) {
	var started bool

	OnContainerStart(func(c *dockerclient.Container) error {
		started = true
		return nil
	})

	cd := &ContainerDefinition{
		dockerclient.CreateContainerOptions{
			Config: &dockerclient.Config{
				Image: "base",
				Cmd:   []string{"/bin/sh", "-c", "while true; do echo hello world; sleep 1; done"},
			},
		},
		dockerclient.HostConfig{},
	}

	cid, err := StartContainer(cd, 30*time.Second)
	if err != nil {
		t.Fatal("can't start container: ", err)
	}

	StopContainer(cid, 10*time.Second)

	if !started {
		t.Fatal("OnContainerStart handler was not triggered")
	}
}

func TestMultipleOnStartContainer(t *testing.T) {
	started := []string{}

	OnContainerStart(func(c *dockerclient.Container) error {
		started = append(started, c.ID)
		return nil
	})

	OnContainerStart(func(c *dockerclient.Container) error {
		started = append(started, c.ID)
		return nil
	})

	cd := &ContainerDefinition{
		dockerclient.CreateContainerOptions{
			Config: &dockerclient.Config{
				Image: "base",
				Cmd:   []string{"/bin/sh", "-c", "while true; do echo hello world; sleep 1; done"},
			},
		},
		dockerclient.HostConfig{},
	}

	cid, err := StartContainer(cd, 30*time.Second)
	if err != nil {
		t.Fatal("can't start container: ", err)
	}

	StopContainer(cid, 10*time.Second)

	if len(started) != 2 {
		t.Fatal("expected OnContainerStart to be trigger count to be 2, not: ", len(started))
	}
}

func TestOnCreateContainer(t *testing.T) {
	var created bool

	OnContainerCreation(func(c *dockerclient.Container) error {
		created = true
		return nil
	})

	cd := &ContainerDefinition{
		dockerclient.CreateContainerOptions{
			Config: &dockerclient.Config{
				Image: "ubuntu",
				Cmd:   []string{"/bin/sh", "-c", "while true; do echo yowza; sleep 1; done"},
			},
		},
		dockerclient.HostConfig{},
	}

	cid, err := StartContainer(cd, 30*time.Second)
	if err != nil {
		t.Fatal("can't start container: ", err)
	}

	StopContainer(cid, 10*time.Second)

	if !created {
		t.Fatal("OnContainerCreated handler was not triggered")
	}
}
