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
	Delete    bool
	version   interface{}
}

func newnode(VirtualIP *pool.VirtualIP) *vipnode {
	return &vipnode{*VirtualIP, "", false, nil}
}

func (node *vipnode) Version() interface{}           { return node.version }
func (node *vipnode) SetVersion(version interface{}) { node.version = version }

type VIPHandler interface {
	BindVirtualIP(vip *pool.VirtualIP) (string, error)
	UnbindVirtualIP(vip *pool.VirtualIP) error
}

type VIPListener struct {
	conn    client.Connection
	handler VIPHandler
}

func NewVIPListener(conn client.Connection, handler VIPHandler) *VIPListener {
	return &VIPListener{conn, handler}
}

func (listener *VIPListener) Listen() {
	shutdown := make(chan interface{})
	defer func() {
		glog.Info("Shutting down all virtual IP listeners")
		close(shutdown)
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

	var (
		done       = make(chan string)
		processing = make(map[string]interface{})
	)

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
				go listener.listenVIP(shutdown, done, ip)
			}
		}

		select {
		case e := <-event:
			glog.V(2).Infof("Received an event: %v", e)
		case ip := <-done:
			glog.V(2).Info("Cleaning up virtual IP ", ip)
			delete(processing, ip)
		}
	}
}

func (listener *VIPListener) listenVIP(shutdown <-chan interface{}, done chan<- string, ip string) {
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

		// Check if the node needs to be deleted
		if node.Delete {
			listener.unbind(&node)
			return
		}

		// Check if the node needs to be created
		if node.HostID == "" {
			if err := listener.bind(&node); err != nil {
				glog.Errorf("Could not bind virtual ip %s: %s", ip, err)
				return
			}
		}

		select {
		case e := <-event:
			glog.V(2).Infof("IP %s received event: %v", ip, e)
		case <-shutdown:
			glog.V(1).Infof("IP %s receieved signal to shutdown")
			listener.unbind(&node)
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

func (listener *VIPListener) bind(node *vipnode) error {
	var err error
	node.HostID, err = listener.handler.BindVirtualIP(&node.VirtualIP)
	if err != nil {
		listener.unbind(node)
		return err
	} else if err := listener.conn.Set(vippath(node.VirtualIP.IP), node); err != nil {
		return err
	}
	return nil
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
			continue
		}

		vpath := vippath(ip)
		var node vipnode
		if err := conn.Get(vpath, &node); err != nil {
			return err
		}
		node.Delete = true
		if err := conn.Set(vpath, &node); err != nil {
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