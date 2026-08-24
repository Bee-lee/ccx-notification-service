/*
Copyright © 2025, 2026 Red Hat, Inc.

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

// Unit tests for the disabled-rules filtering in the Kafka processing path
// (CCXDEV-16567). They cover the isRuleDisabled helper directly and the
// produceEntriesToKafka integration that must skip a disabled rule before it
// reaches the total risk filter or ShouldNotify.

import (
	"testing"

	utypes "github.com/RedHatInsights/insights-results-types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// disabledRulesCluster is the cluster used across the disabled-rules tests.
var disabledRulesCluster = types.ClusterEntry{
	OrgID:         1,
	AccountNumber: 1,
	ClusterName:   "first_cluster",
	KafkaOffset:   0,
}

// --- isRuleDisabled unit tests ---

// TestIsRuleDisabledClusterLevelMatch verifies that a rule present in the
// cluster-level map (cluster_rule_toggle) is reported as disabled.
func TestIsRuleDisabledClusterLevelMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "first_cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.True(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "RULE_1"))
}

// TestIsRuleDisabledOrgLevelMatch verifies that a rule present in the org-level
// map (rule_disable) is reported as disabled.
func TestIsRuleDisabledOrgLevelMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	assert.True(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "RULE_1"))
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule missing from both maps
// is not reported as disabled.
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "RULE_1"))
}

// TestIsRuleDisabledClusterLevelOtherCluster verifies that a cluster-level
// disable for a different cluster does not disable the rule for this cluster.
// Mirrors the BDD scenario where a single-cluster disable only affects that
// cluster.
func TestIsRuleDisabledClusterLevelOtherCluster(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "other_cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "RULE_1"))
}

// TestIsRuleDisabledOrgLevelOtherOrg verifies that an org-wide ack for a
// different organization does not disable the rule for this org's cluster.
// Mirrors the BDD scenario where an org ack does not affect other orgs.
func TestIsRuleDisabledOrgLevelOtherOrg(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "2", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "RULE_1"))
}

// TestIsRuleDisabledErrorKeyMismatch verifies that a matching rule id but a
// different error key is not reported as disabled (the composite key must match
// on both rule id and error key).
func TestIsRuleDisabledErrorKeyMismatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "first_cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesCluster, "rule_1", "OTHER_KEY"))
}

// --- produceEntriesToKafka integration tests ---

// makeReportContent builds a single-rule report referring to rule_1/RULE_1.
func makeReportContent() types.ReportContent {
	return types.ReportContent{
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_1.report",
				ErrorKey: "RULE_1",
			},
		},
	}
}

// makeRuleContent builds rule content for rule_1/RULE_1 with a total risk high
// enough to pass the default total risk filter.
func makeRuleContent() types.RulesMap {
	return types.RulesMap{
		"rule_1": {
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"RULE_1": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "rule 1 error key description",
						Impact: utypes.Impact{
							Name:   "impact_1",
							Impact: 4,
						},
						Likelihood: 4,
					},
				},
			},
		},
	}
}

// newProducerMock returns a producer mock that reports a successful send with a
// valid (non -1) Kafka offset.
func newProducerMock() *mocks.Producer {
	producerMock := &mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)
	return producerMock
}

// TestProduceEntriesToKafkaRuleNotDisabledIsNotified verifies the baseline:
// a rule that is not disabled passes the total risk filter and ShouldNotify,
// and a notification is produced.
func TestProduceEntriesToKafkaRuleNotDisabledIsNotified(t *testing.T) {
	producerMock := newProducerMock()

	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	d := differ.Differ{
		Storage:              storage,
		Notifier:             producerMock,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	count, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesCluster, makeRuleContent(), makeReportContent(), types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 1, count, "an enabled rule above the threshold should be notified")
	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
}

// TestProduceEntriesToKafkaClusterDisabledRuleSkipped verifies that a rule
// present in the cluster-level map is skipped entirely: no notification is
// produced and the rule never reaches the producer.
func TestProduceEntriesToKafkaClusterDisabledRuleSkipped(t *testing.T) {
	producerMock := newProducerMock()

	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	d := differ.Differ{
		Storage:          storage,
		Notifier:         producerMock,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "first_cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	count, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesCluster, makeRuleContent(), makeReportContent(), types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, count, "a cluster-disabled rule must not be notified")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaOrgDisabledRuleSkipped verifies that a rule present
// in the org-level map (rule ack) is skipped entirely: no notification is
// produced and the rule never reaches the producer.
func TestProduceEntriesToKafkaOrgDisabledRuleSkipped(t *testing.T) {
	producerMock := newProducerMock()

	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	d := differ.Differ{
		Storage:              storage,
		Notifier:             producerMock,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	count, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesCluster, makeRuleContent(), makeReportContent(), types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, count, "an org-acked rule must not be notified")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaDisabledRuleSkippedNonDisabledNotified verifies the
// ordering guarantee across a mixed report: with two rules above the threshold
// where only one is disabled, only the enabled rule is notified. This proves
// the disabled rule is filtered before the total risk filter / ShouldNotify.
func TestProduceEntriesToKafkaDisabledRuleSkippedNonDisabledNotified(t *testing.T) {
	producerMock := newProducerMock()

	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	reportItems := types.ReportContent{
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_1.report",
				ErrorKey: "RULE_1",
			},
		},
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_2.report",
				ErrorKey: "RULE_2",
			},
		},
	}

	ruleContent := types.RulesMap{
		"rule_1": {
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"RULE_1": {
					Metadata: utypes.ErrorKeyMetadata{
						Impact:     utypes.Impact{Impact: 4},
						Likelihood: 4,
					},
				},
			},
		},
		"rule_2": {
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"RULE_2": {
					Metadata: utypes.ErrorKeyMetadata{
						Impact:     utypes.Impact{Impact: 4},
						Likelihood: 4,
					},
				},
			},
		},
	}

	d := differ.Differ{
		Storage:          storage,
		Notifier:         producerMock,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "first_cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	count, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesCluster, ruleContent, reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 1, count, "only the enabled rule should be notified")
	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
}
