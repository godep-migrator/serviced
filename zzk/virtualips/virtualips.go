package virtualips

import (
	"path"

	"github.com/zenoss/glog"
	"github.com/zenoss/serviced/coordinator/client"
	"github.com/zenoss/serviced/domain/pool"
)

const (
	zkVirtualIPs = "/VirtualIPs"
)

func vippath(nodes ...string) string {
	p := append([]string{zkVirtualIPs}, nodes...)
	return path.Join(p...)
}

type vipnode struct {
	VirtualIP pool.VirtualIP
	HostID    string
	version   interface{}
}

func newnode(VirtualIP *pool.VirtualIP) *vipnode {
	return &vipnode{*VirtualIP, "", nil}
}

func (node *vipnode) Version() interface{}           { return node.version }
func (node *vipnode) SetVersion(version interface{}) { node.version = version }

type VIPHandler interface {
	BindVirtualIP(vip *pool.VirtualIP, index int) error
	UnbindVirtualIP(vip *pool.VirtualIP) error
}

type VIPListener struct {
	conn    client.Connection
	handler VIPHandler
	hostID  string
}

func NewVIPListener(conn client.Connection, handler VIPHandler, hostID string) *VIPListener {
	return &VIPListener{conn, handler, hostID}
}

func (listener *VIPListener) Listen(shutdown <-chan interface{}) {
	var (
		_shutdown  = make(chan interface{})
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

	defer func() {
		glog.Info("Shutting down all virtual IP listeners")
		close(_shutdown)
		for len(processing) > 0 {
			delete(processing, <-done)
		}
	}()

	if exists, err := listener.conn.Exists(vippath()); err != nil {
		glog.Error("Unable to look up virtual IP path on zookeeper: ", err)
		return
	} else if exists {
		// TODO: remove all virtual IPs that may be present before starting the loop
	}
	if err := listener.conn.CreateDir(vippath()); err != nil {
		glog.Error("Unable to create virtual IP path on zookeeper: ", err)
	}

	vipIndex := 0
	for {
		vips, event, err := listener.conn.ChildrenW(vippath())
		if err != nil {
			glog.Error("Unable to watch virtual IPs: ", err)
			return
		}

		for _, ip := range vips {
			if _, ok := processing[ip]; !ok {
				glog.V(1).Info("Spawning a listener for IP ", ip)
				processing[ip] = nil
				go listener.listenVIP(_shutdown, done, ip, vipIndex)
				vipIndex++
			}
		}

		select {
		case e := <-event:
			glog.V(2).Infof("Received an event: %v", e)
		case ip := <-done:
			glog.V(2).Info("Cleaning up virtual IP ", ip)
			delete(processing, ip)
		case <-shutdown:
			return
		}
	}
}

func (listener *VIPListener) listenVIP(shutdown <-chan interface{}, done chan<- string, ip string, index int) {
	defer func() {
		glog.V(2).Info("Shutting down listener for virtual IP ", ip)
		done <- ip
	}()

	for {
		var node vipnode
		event, err := listener.conn.GetW(vippath(ip), &node)
		if err != nil {
			glog.Errorf("Could not load virtual ip %s: %s", ip, err)
			return
		}

		// Check if the node needs to be created
		if node.HostID == "" {
			if err := listener.bind(&node, index); err != nil {
				glog.Errorf("Could not bind virtual ip %s: %s", ip, err)
				return
			}
		}

		select {
		case e := <-event:
			if e.Type == client.EventNodeDeleted {
				glog.V(1).Infof("Shutting down due to virtual IP delete %s", ip)
				listener.unbind(&node)
				return
			}
			glog.V(2).Infof("IP %s received event: %v", ip, e)
		case <-shutdown:
			glog.V(1).Infof("IP %s receieved signal to shutdown")
			listener.unbind(&node)
			return
		}
	}
}

func (listener *VIPListener) unbind(node *vipnode) {
	if err := listener.handler.UnbindVirtualIP(&node.VirtualIP); err != nil {
		glog.Errorf("Could not unbind virtual ip %s: %s", node.VirtualIP.IP, err)
	} else if err := listener.conn.Delete(vippath(node.VirtualIP.IP)); err != nil {
		glog.Errorf("Could not delete virtual ip %s: %s", node.VirtualIP.IP, err)
	}
}

func (listener *VIPListener) bind(node *vipnode, index int) error {
	if err := listener.handler.BindVirtualIP(&node.VirtualIP, index); err != nil {
		listener.unbind(node)
		return err
	}
	node.HostID = listener.hostID
	return listener.conn.Set(vippath(node.VirtualIP.IP), node)
}

func Sync(conn client.Connection, vips []pool.VirtualIP) error {
	availableVIPs, err := conn.Children(vippath())
	if err != nil {
		return err
	}

	unsyncedVIPs := make(map[string]*pool.VirtualIP)
	for i, vip := range vips {
		unsyncedVIPs[vip.IP] = &vips[i]
	}

	// delete all non-matching vips
	for _, ip := range availableVIPs {
		if _, ok := unsyncedVIPs[ip]; ok {
			delete(unsyncedVIPs, ip)
		} else if err := conn.Delete(vippath(ip)); err != nil {
			return err
		}
	}

	// add whatever remains unsynced
	for ip, vip := range unsyncedVIPs {
		if err := conn.Create(vippath(ip), newnode(vip)); err != nil {
			return err
		}
	}

	return nil
}

func GetHost(conn client.Connection, ip string) (string, error) {
	var node vipnode
	if err := conn.Get(vippath(ip), &node); err != nil {
		return "", err
	}
	return node.HostID, nil
}