package serviced

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/domain/pool"
)

const (
	viPrefix = ":zvip"
)

func bind(vip *pool.VirtualIP, name string) error {
	if err := exec.Command("ifconfig", name, "inet", vip.IP, "netmask", vip.Netmask).Run(); err != nil {
		return fmt.Errorf("could not create virtual interface %s", name)
	}
	glog.Infof("Added virtual interface/IP: %s (%v)", name, vip)
	return nil
}

func unbind(name string) error {
	if out, err := exec.Command("ifconfig", name, "down").CombinedOutput(); err != nil {
		return fmt.Errorf("could not unbind virtual interface: %s", out)
	}
	glog.Info("Removed virtual interface: ", name)
	return nil
}

func mapVirtualIPs() (map[string]*pool.VirtualIP, error) {
	vmap := make(map[string]*pool.VirtualIP)

	for _, viname := range getVirtualInterfaceNames() {
		vip, err := lookupVirtualIP(viname)
		if err != nil {
			return nil, err
		}
		vmap[vip.IP] = vip
	}

	return vmap, nil
}

func getVirtualInterfaceNames() (names []string) {
	viNamesCmd := "ifconfig | awk '/" + viPrefix + "/{print $1}'"
	cmd := exec.Command("bash", "-c", viNamesCmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		glog.Warningf("Could not get virtual interface names: %s", out)
		return
	}
	return strings.Fields(string(out))
}

func lookupVirtualIP(name string) (*pool.VirtualIP, error) {
	bindInterfaceAndIndex := strings.Split(name, viPrefix)
	if len(bindInterfaceAndIndex) != 2 {
		return nil, fmt.Errorf("unexpected interface format")
	}
	bindInterface := strings.TrimSpace(bindInterfaceAndIndex[0])

	// ifconfig eth0 | awk '/inet addr:/{print $2}' | cut -d: -f2
	// 10.87.110.175
	vipCmd := fmt.Sprintf("ifconfig %s | awk 'inet addr:/{print $2} | cut -d: -f2", name)
	cmd := exec.Command("bash", "-c", vipCmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("virtual ip not found: %s", out)
	}

	return &pool.VirtualIP{
		IP:            strings.TrimSpace(string(out)),
		BindInterface: bindInterface,
		InterfaceName: name,
	}, nil
}