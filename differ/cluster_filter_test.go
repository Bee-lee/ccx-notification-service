/*
Copyright © 2022 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package differ

// Documentation in literate-programming-style is available at:
// https://redhatinsights.github.io/ccx-notification-writer/packages/differ/cluster_filter_test.html

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// cluster entries to be used by unit tests
var (
	cluster1 = types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 2,
		ClusterName:   "aaaaaaaa-0000-0000-0000-00000000000",
		KafkaOffset:   0,
	}
	cluster2 = types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 2,
		ClusterName:   "bbbbbbbb-0000-0000-0000-00000000000",
		KafkaOffset:   0,
	}
	cluster3 = types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 2,
		ClusterName:   "cccccccc-0000-0000-0000-00000000000",
		KafkaOffset:   0,
	}
	cluster4 = types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 2,
		ClusterName:   "dddddddd-0000-0000-0000-00000000000",
		KafkaOffset:   0,
	}
	cluster5 = types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 2,
		ClusterName:   "eeeeeeee-0000-0000-0000-00000000000",
		KafkaOffset:   0,
	}
)

// TestFilterNullClusterList test checks the filtering for null cluster list
func TestFilterNullClusterList(t *testing.T) {
	config := conf.ProcessingConfiguration{
		FilterAllowedClusters: false,
		FilterBlockedClusters: false,
	}

	// null value
	var clusters []types.ClusterEntry

	// start filter
	filtered, stat := filterClusterList(clusters, config)

	// check filter output
	assert.Empty(t, filtered)
	assert.Equal(t, 0, stat.Input)
	assert.Equal(t, 0, stat.Allowed)
	assert.Equal(t, 0, stat.Blocked)
	assert.Equal(t, 0, stat.Filtered)
}

// TestFilterEmptyClusterList test checks the filtering for null cluster list
func TestFilterEmptyClusterList(t *testing.T) {
	config := conf.ProcessingConfiguration{
		FilterAllowedClusters: false,
		FilterBlockedClusters: false,
	}

	// empty cluster list
	clusters := []types.ClusterEntry{}

	// start filter
	filtered, stat := filterClusterList(clusters, config)

	// check filter output
	assert.Empty(t, filtered)
	assert.Equal(t, 0, stat.Input)
	assert.Equal(t, 0, stat.Allowed)
	assert.Equal(t, 0, stat.Blocked)
	assert.Equal(t, 0, stat.Filtered)
}
