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
