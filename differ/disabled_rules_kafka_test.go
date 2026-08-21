/*
Copyright © 2026 Red Hat, Inc.

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

package differ_test

// Tests for filtering disabled rules in the Kafka processing path
// (CCXDEV-16567). The disabled rules check (isRuleDisabled) short-circuits the
// per-rule loop in produceEntriesToKafka before the total risk filter or
// ShouldNotify are evaluated.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
	"github.com/RedHatInsights/insights-results-aggregator-data/testdata"
)

// rule identities present in testdata.ClusterReport3Rules after the report JSON
// is parsed. The report stores the fully qualified module name in "component";
// moduleToRuleName reduces it to the last dotted segment, which is the rule_id
// used to match against the disabled rules maps.
const (
	kafkaRule1Name = types.RuleID("node_installer_degraded")
	kafkaRule1Key  = types.ErrorKey("ek1")
	kafkaRule2Name = types.RuleID("rule2")
	kafkaRule2Key  = types.ErrorKey("ek2")
	kafkaRule3Name = types.RuleID("rule3")
	kafkaRule3Key  = types.ErrorKey("ek3")

	kafkaTestOrgID uint32 = 1
)

// buildEvaluatedReportItem builds a single report item matching one of the
// three rules found in testdata.ClusterReport3Rules.
func buildEvaluatedReportItem(module types.ModuleName, errorKey types.ErrorKey) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			Module:   module,
			ErrorKey: errorKey,
		},
	}
}

// newKafkaTestCluster returns a cluster entry matching the disabled rules keys
// used across the Kafka disabled rules tests.
func newKafkaTestCluster() types.ClusterEntry {
	return types.ClusterEntry{
		OrgID:         types.OrgID(kafkaTestOrgID),
		AccountNumber: 1,
		ClusterName:   types.ClusterName(testdata.ClusterName),
		UpdatedAt:     types.Timestamp(testTimestamp),
	}
}

// --- isRuleDisabled unit tests -------------------------------------------------

// TestIsRuleDisabledClusterLevel verifies that a rule present in the
// cluster-level (cluster_rule_toggle) map is reported as disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.True(t, differ.IsRuleDisabled(&d, cluster, item),
		"rule present in the cluster-level map must be reported as disabled")
}

// TestIsRuleDisabledOrgLevel verifies that a rule present in the org-level
// (rule_disable) map is reported as disabled.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
		},
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.True(t, differ.IsRuleDisabled(&d, cluster, item),
		"rule present in the org-level map must be reported as disabled")
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule absent from both maps
// is not reported as disabled and thus would proceed to the total risk filter.
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.False(t, differ.IsRuleDisabled(&d, cluster, item),
		"rule absent from both maps must not be reported as disabled")
}

// TestIsRuleDisabledClusterKeyIsScopedToCluster verifies that a cluster-level
// disable for one cluster does not affect a different cluster.
func TestIsRuleDisabledClusterKeyIsScopedToCluster(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: types.ClusterName("some-other-cluster"), RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.False(t, differ.IsRuleDisabled(&d, cluster, item),
		"a disable scoped to a different cluster must not match")
}

// TestIsRuleDisabledOrgKeyIsScopedToOrg verifies that an org-level disable for
// a different organization does not affect this cluster's org.
func TestIsRuleDisabledOrgKeyIsScopedToOrg(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "9999", RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
		},
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.False(t, differ.IsRuleDisabled(&d, cluster, item),
		"a disable scoped to a different org must not match")
}

// TestIsRuleDisabledErrorKeyMustMatch verifies that matching happens on the
// composite (rule_id, error_key) and not on the rule_id alone.
func TestIsRuleDisabledErrorKeyMustMatch(t *testing.T) {
	cluster := newKafkaTestCluster()

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: kafkaRule1Name, ErrorKey: types.ErrorKey("different_key")}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	item := buildEvaluatedReportItem(types.ModuleName(testdata.Rule1ID), kafkaRule1Key)
	assert.False(t, differ.IsRuleDisabled(&d, cluster, item),
		"a disable for the same rule but a different error key must not match")
}

// --- produceEntriesToKafka integration tests ----------------------------------

// setupKafkaDiffer builds a Differ wired with a permissive filter, an empty
// cooldown (so ShouldNotify always returns true), and the given disabled rules
// maps. The producer and storage mocks are returned so the caller can assert on
// the number of produced messages.
func setupKafkaDiffer(clusterDisabled types.ClusterDisabledRules, orgDisabled types.OrgDisabledRules) (differ.Differ, *mocks.Producer, *mocks.Storage) {
	producerMock := &mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	storageMock := &mocks.Storage{}
	storageMock.On("WriteNotificationRecordForCluster",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(nil)

	d := differ.Differ{
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Filter:               differ.DefaultEventFilter,
		Notifier:             producerMock,
		Storage:              storageMock,
		PreviouslyReported:   make(types.NotifiedRecordsPerCluster),
		ClusterDisabledRules: clusterDisabled,
		OrgDisabledRules:     orgDisabled,
	}
	return d, producerMock, storageMock
}

// kafkaRuleContent returns the rule content map covering all three rules in
// testdata.ClusterReport3Rules.
func kafkaRuleContent() types.RulesMap {
	return types.RulesMap{
		"node_installer_degraded": testdata.RuleContent1,
		"rule2":                   testdata.RuleContent2,
		"rule3":                   testdata.RuleContent3,
	}
}

// parseKafka3Rules parses testdata.ClusterReport3Rules into report items.
func parseKafka3Rules(t *testing.T) types.ReportContent {
	var deserialized types.Report
	err := json.Unmarshal([]byte(testdata.ClusterReport3Rules), &deserialized)
	assert.NoError(t, err)
	return deserialized.Reports
}

// TestProduceEntriesToKafkaClusterDisabledRuleSkipped verifies that a rule
// disabled at the cluster level is skipped, so only the two remaining rules
// produce notification events.
func TestProduceEntriesToKafkaClusterDisabledRuleSkipped(t *testing.T) {
	cluster := newKafkaTestCluster()
	clusterDisabled := types.ClusterDisabledRules{
		{ClusterID: cluster.ClusterName, RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
	}
	d, _, _ := setupKafkaDiffer(clusterDisabled, make(types.OrgDisabledRules))

	messages, err := differ.ProduceEntriesToKafka(&d, cluster, kafkaRuleContent(),
		parseKafka3Rules(t), types.ClusterReport(testdata.ClusterReport3Rules))

	assert.NoError(t, err)
	assert.Equal(t, 2, messages,
		"the cluster-disabled rule must be skipped, leaving 2 notified rules")
}

// TestProduceEntriesToKafkaOrgDisabledRuleSkipped verifies that a rule disabled
// org-wide is skipped, so only the two remaining rules produce events.
func TestProduceEntriesToKafkaOrgDisabledRuleSkipped(t *testing.T) {
	cluster := newKafkaTestCluster()
	orgDisabled := types.OrgDisabledRules{
		{OrgID: "1", RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
	}
	d, _, _ := setupKafkaDiffer(make(types.ClusterDisabledRules), orgDisabled)

	messages, err := differ.ProduceEntriesToKafka(&d, cluster, kafkaRuleContent(),
		parseKafka3Rules(t), types.ClusterReport(testdata.ClusterReport3Rules))

	assert.NoError(t, err)
	assert.Equal(t, 2, messages,
		"the org-disabled rule must be skipped, leaving 2 notified rules")
}

// TestProduceEntriesToKafkaNoDisabledRules verifies that when no rule is
// disabled, all three rules proceed through the total risk filter and produce
// notification events.
func TestProduceEntriesToKafkaNoDisabledRules(t *testing.T) {
	cluster := newKafkaTestCluster()
	d, _, _ := setupKafkaDiffer(make(types.ClusterDisabledRules), make(types.OrgDisabledRules))

	messages, err := differ.ProduceEntriesToKafka(&d, cluster, kafkaRuleContent(),
		parseKafka3Rules(t), types.ClusterReport(testdata.ClusterReport3Rules))

	assert.NoError(t, err)
	assert.Equal(t, 3, messages,
		"with no disabled rules all 3 rules must be notified")
}

// TestProduceEntriesToKafkaAllRulesDisabled verifies that when every rule is
// disabled, no events are produced and no message is sent to Kafka. This proves
// disabled rules never reach ShouldNotify or the producer.
func TestProduceEntriesToKafkaAllRulesDisabled(t *testing.T) {
	cluster := newKafkaTestCluster()
	clusterDisabled := types.ClusterDisabledRules{
		{ClusterID: cluster.ClusterName, RuleID: kafkaRule1Name, ErrorKey: kafkaRule1Key}: {},
		{ClusterID: cluster.ClusterName, RuleID: kafkaRule2Name, ErrorKey: kafkaRule2Key}: {},
		{ClusterID: cluster.ClusterName, RuleID: kafkaRule3Name, ErrorKey: kafkaRule3Key}: {},
	}
	d, producerMock, _ := setupKafkaDiffer(clusterDisabled, make(types.OrgDisabledRules))

	messages, err := differ.ProduceEntriesToKafka(&d, cluster, kafkaRuleContent(),
		parseKafka3Rules(t), types.ClusterReport(testdata.ClusterReport3Rules))

	assert.NoError(t, err)
	assert.Equal(t, 0, messages, "with every rule disabled no events must be produced")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}
