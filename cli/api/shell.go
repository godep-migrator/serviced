package api

import (
	"fmt"
	"os"
	"strings"

	"github.com/zenoss/glog"
	docker "github.com/zenoss/go-dockerclient"
	"github.com/zenoss/serviced/domain/service"
	"github.com/zenoss/serviced/shell"
)

// ShellConfig is the deserialized object from the command-line
type ShellConfig struct {
	ServiceID string
	Command   string
	Args      []string
	SaveAs    string
	IsTTY     bool
}

// StartShell runs a command for a given service
func (a *api) StartShell(config ShellConfig) error {
	command := []string{config.Command}
	command = append(command, config.Args...)

	cfg := shell.ProcessConfig{
		ServiceId: config.ServiceID,
		IsTTY:     config.IsTTY,
		SaveAs:    config.SaveAs,
		Command:   strings.Join(command, " "),
	}

	// TODO: change me to use sockets
	cmd, err := shell.StartDocker(&cfg, options.Port)
	if err != nil {
		return fmt.Errorf("failed to connect to service: %s", err)
	}

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()

	return nil
}

// RunShell runs a predefined service shell command via the service definition
func (a *api) RunShell(config ShellConfig) error {
	client, err := a.connectDAO()
	if err != nil {
		return err
	}

	svc, err := a.GetService(config.ServiceID)
	if err != nil {
		return err
	}

	getSvc := func(svcID string) (service.Service, error) {
		s := service.Service{}
		err := client.GetService(svcID, &s)
		return s, err
	}
	if err := svc.EvaluateRunsTemplate(getSvc); err != nil {
		fmt.Errorf("error evaluating service:%s Runs:%+v  error:%s", svc.Id, svc.Runs, err)
	}
	command, ok := svc.Runs[config.Command]
	if !ok {
		return fmt.Errorf("command not found for service")
	}
	command = strings.Join(append([]string{command}, config.Args...), " ")

	cfg := shell.ProcessConfig{
		ServiceId: config.ServiceID,
		IsTTY:     config.IsTTY,
		SaveAs:    config.SaveAs,
		Command:   fmt.Sprintf("su - zenoss -c \"%s\"", command),
	}

	// TODO: change me to use sockets
	cmd, err := shell.StartDocker(&cfg, options.Port)
	if err != nil {
		return fmt.Errorf("failed to connect to service: %s", err)
	}

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		glog.Fatalf("abnormal termination from shell command: %s", err)
	}

	dockercli, err := a.connectDocker()
	if err != nil {
		glog.Fatalf("unable to connect to the docker service: %s", err)
	}
	exitcode, err := dockercli.WaitContainer(config.SaveAs)
	if err != nil {
		glog.Fatalf("failure waiting for container: %s", err)
	}
	container, err := dockercli.InspectContainer(config.SaveAs)
	if err != nil {
		glog.Fatalf("cannot acquire information about container: %s (%s)", config.SaveAs, err)
	}
	glog.V(2).Infof("Container ID: %s", container.ID)

	switch exitcode {
	case 0:
		// Commit the container
		label := ""
		glog.V(0).Infof("Committing container")
		if err := client.Commit(container.ID, &label); err != nil {
			glog.Fatalf("Error committing container: %s (%s)", container.ID, err)
		}
	default:
		// Delete the container
		if err := dockercli.StopContainer(container.ID, 10); err != nil {
			glog.Fatalf("failed to stop container: %s (%s)", container.ID, err)
		} else if err := dockercli.RemoveContainer(docker.RemoveContainerOptions{ID: container.ID}); err != nil {
			glog.Fatalf("failed to remove container: %s (%s)", container.ID, err)
		}
	}

	return nil
}
