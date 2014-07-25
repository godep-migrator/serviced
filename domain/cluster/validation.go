// Copyright 2014, The Serviced Authors. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/control-center/serviced/validation"
	"github.com/zenoss/glog"

	"strings"
)

//ValidEntity validates Cluster fields
func (c *Cluster) ValidEntity() error {
	glog.V(4).Info("Validating Cluster")

	trimmedID := strings.TrimSpace(c.ID)
	violations := validation.NewValidationError()
	violations.Add(validation.NotEmpty("Cluster.ID", c.ID))
	violations.Add(validation.StringsEqual(c.ID, trimmedID, "leading and trailing spaces not allowed for pool id"))

	if len(violations.Errors) > 0 {
		return violations
	}
	return nil
}
