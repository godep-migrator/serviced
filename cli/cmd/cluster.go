// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/control-center/serviced/cli/api"
	"github.com/control-center/serviced/domain/cluster"
)

// Initializer for serviced cluster subcommands
func (c *ServicedCli) initPool() {
	c.app.Commands = append(c.app.Commands, cli.Command{
		Name:        "cluster",
		Usage:       "Administers cluster data",
		Description: "",
		Subcommands: []cli.Command{
			{
				Name:         "list",
				Usage:        "Lists all clusters",
				Description:  "serviced cluster list [CLUSTERID]",
				BashComplete: c.printClustersFirst,
				Action:       c.cmdPoolList,
				Flags: []cli.Flag{
					cli.BoolFlag{"verbose, v", "Show JSON format"},
				},
			}, {
				Name:         "add",
				Usage:        "Adds a new cluster",
				Description:  "serviced cluster add CLUSTERID",
				BashComplete: nil,
				Action:       c.cmdClusterAdd,
			}, {
				Name:         "remove",
				ShortName:    "rm",
				Usage:        "Removes an existing cluster",
				Description:  "serviced cluster remove CLUSTERID ...",
				BashComplete: c.printPoolsAll,
				Action:       c.cmdPoolRemove,
			}, {
				Name:         "list-pools",
				Usage:        "Lists the pools in a cluster",
				Description:  "serviced cluster list-pools CLUSTERID",
				BashComplete: c.printClustersFirst,
				Action:       c.cmdPoolListIPs,
				Flags: []cli.Flag{
					cli.BoolFlag{"verbose, v", "Show JSON format"},
				},
			},
		},
	})
}

// Returns a list of available pools
func (c *ServicedCli) pools() (data []string) {
	pools, err := c.driver.GetResourcePools()
	if err != nil || pools == nil || len(pools) == 0 {
		return
	}

	data = make([]string, len(pools))
	for i, p := range pools {
		data[i] = p.ID
	}

	return
}

// Bash-completion command that prints the list of available clusters as the
// first argument
func (c *ServicedCli) printClustersFirst(ctx *cli.Context) {
	if len(ctx.Args()) > 0 {
		return
	}
	fmt.Println(strings.Join(c.clusters(), "\n"))
}

// Bash-completion command that prints the list of available pools as all
// arguments
func (c *ServicedCli) printPoolsAll(ctx *cli.Context) {
	args := ctx.Args()
	pools := c.pools()

	for _, p := range pools {
		for _, a := range args {
			if p == a {
				goto next
			}
		}
		fmt.Println(p)
	next:
	}
}

// serviced cluster list [CLUSTERID]
func (c *ServicedCli) cmdClusterList(ctx *cli.Context) {
	if len(ctx.Args()) > 0 {
		clusterID := ctx.Args()[0]
		if pool, err := c.driver.GetCluster(clusterID); err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else if pool == nil {
			fmt.Fprintln(os.Stderr, "pool not found")
		} else if jsonPool, err := json.MarshalIndent(pool, " ", "  "); err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal resource pool: %s", err)
		} else {
			fmt.Println(string(jsonPool))
		}
		return
	}

	pools, err := c.driver.GetResourcePools()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	} else if pools == nil || len(pools) == 0 {
		fmt.Fprintln(os.Stderr, "no resource pools found")
		return
	}

	if ctx.Bool("verbose") {
		if jsonPool, err := json.MarshalIndent(pools, " ", "  "); err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal resource pool list: %s", err)
		} else {
			fmt.Println(string(jsonPool))
		}
	} else {
		tablePool := newtable(0, 8, 2)
		tablePool.printrow("ID", "PARENT" /*"CORE", "MEM",*/, "PRI")
		for _, p := range pools {
			tablePool.printrow(p.ID, p.ParentID /*p.CoreLimit, p.MemoryLimit,*/, p.Priority)
		}
		tablePool.flush()
	}
}

// serviced pool add POOLID PRIORITY
func (c *ServicedCli) cmdClusterAdd(ctx *cli.Context) {
	args := ctx.Args()
	if len(args) < 2 {
		fmt.Printf("Incorrect Usage.\n\n")
		cli.ShowCommandHelp(ctx, "add")
		return
	}

	var err error

	cfg := api.PoolConfig{}
	cfg.ClusterID = args[0]

	if pool, err := c.driver.AddCluster(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
	} else if pool == nil {
		fmt.Fprintln(os.Stderr, "received nil cluster")
	} else {
		fmt.Println(cluster.ID)
	}
}

// serviced pool remove POOLID ...
func (c *ServicedCli) cmdPoolRemove(ctx *cli.Context) {
	args := ctx.Args()
	if len(args) < 1 {
		fmt.Printf("Incorrect Usage.\n\n")
		cli.ShowCommandHelp(ctx, "remove")
	}

	for _, id := range args {
		if p, err := c.driver.GetResourcePool(id); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", id, err)
		} else if p == nil {
			fmt.Fprintf(os.Stderr, "%s: pool not found", id)
		} else if err := c.driver.RemoveResourcePool(id); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", id, err)
		} else {
			fmt.Println(id)
		}
	}
}

// serviced pool list-ips POOLID
func (c *ServicedCli) cmdPoolListIPs(ctx *cli.Context) {
	args := ctx.Args()
	if len(args) < 1 {
		fmt.Printf("Incorrect Usage.\n\n")
		cli.ShowCommandHelp(ctx, "list-ips")
		return
	}

	if poolIps, err := c.driver.GetPoolIPs(args[0]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	} else if poolIps.HostIPs == nil || (len(poolIps.HostIPs) == 0 && len(poolIps.VirtualIPs) == 0) {
		fmt.Fprintln(os.Stderr, "no resource pool IPs found")
		return
	} else if ctx.Bool("verbose") {
		if jsonPoolIP, err := json.MarshalIndent(poolIps.HostIPs, " ", "  "); err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal resource pool IPs: %s", err)
		} else {
			fmt.Println(string(jsonPoolIP))
		}
	} else {
		tableIPs := newtable(0, 10, 2)
		tableIPs.printrow("Interface Name", "IP Address", "Type")
		for _, ip := range poolIps.HostIPs {
			tableIPs.printrow(ip.InterfaceName, ip.IPAddress, "static")
		}
		for _, ip := range poolIps.VirtualIPs {
			tableIPs.printrow("", ip.IP, "virtual")
		}
		tableIPs.flush()
	}
}
