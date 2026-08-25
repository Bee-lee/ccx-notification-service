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

// Documentation in literate-programming-style is available at:
// https://redhatinsights.github.io/ccx-notification-writer/packages/differ/disabled_rules_kafka_test.html

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	utypes "github.com/RedHatInsights/insights-results-types"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// disabledRulesTestCluster is the cluster entry used across the tests in
// this file for the Kafka disabled-rules filtering path.
var disabledRulesTestCluster = types.ClusterEntry{
	OrgID:         1,
	AccountNumber: 0o123456,
	ClusterName:   "test_cluster1",
	KafkaOffset:   0,
	UpdatedAt:     types.Timestamp(testTimestamp),
}

// --- moduleToRuleID tests ---

// TestModuleToRuleIDStripsReportSuffix checks that moduleToRuleID converts
// the report JSON's `component` value into the rule_id part of the
// composite `rule_id|error_key` format, i.e. it strips the trailing
// ".report" suffix and nothing else.
func TestModuleToRuleIDStripsReportSuffix(t *testing.T) {
	module := types.ModuleName("ccx_rules_ocp.external.rules.cluster_wide_proxy_auth_check.report")
	expected := types.RuleID("ccx_rules_ocp.external.rules.cluster_wide_proxy_auth_check")
	assert.Equal(t, expected, differ.ModuleToRuleID(module))
}

// --- isRuleDisabled tests ---

// TestIsRuleDisabledClusterMatch verifies that a rule present in the
// cluster-level disabled rules map (populated from cluster_rule_toggle) is
// reported as disabled.
func TestIsRuleDisabledClusterMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: disabledRulesTestCluster.ClusterName,
				RuleID:    "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey:  "RULE_1_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.True(t, disabled, "rule present in cluster-level map should be considered disabled")
}

// TestIsRuleDisabledOrgMatch verifies that a rule present in the org-level
// disabled (acked) rules map (populated from rule_disable) is reported as
// disabled.
func TestIsRuleDisabledOrgMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{},
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "1",
				RuleID:   "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey: "RULE_1_ERROR_KEY",
			}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.True(t, disabled, "rule present in org-level map should be considered disabled")
}

// TestIsRuleDisabledNoMatch verifies that a rule absent from both maps is
// not considered disabled.
func TestIsRuleDisabledNoMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: disabledRulesTestCluster.ClusterName,
				RuleID:    "ccx_rules_ocp.external.rules.rule_2",
				ErrorKey:  "RULE_2_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "1",
				RuleID:   "ccx_rules_ocp.external.rules.rule_3",
				ErrorKey: "RULE_3_ERROR_KEY",
			}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.False(t, disabled, "rule absent from both maps should not be considered disabled")
}

// TestIsRuleDisabledOtherClusterDoesNotMatch verifies that the cluster-level
// map lookup is scoped to the specific cluster: a disabled entry for a
// different cluster must not affect this cluster's rules.
func TestIsRuleDisabledOtherClusterDoesNotMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: "some_other_cluster",
				RuleID:    "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey:  "RULE_1_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.False(t, disabled, "disabled entry for a different cluster should not match")
}

// TestIsRuleDisabledOtherOrgDoesNotMatch verifies that the org-level map
// lookup is scoped to the specific org: an acked entry for a different org
// must not affect this org's rules.
func TestIsRuleDisabledOtherOrgDoesNotMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{},
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "999",
				RuleID:   "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey: "RULE_1_ERROR_KEY",
			}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.False(t, disabled, "acked entry for a different org should not match")
}

// TestIsRuleDisabledErrorKeyMismatchDoesNotMatch verifies that matching
// requires the error_key to match exactly: the same rule_id with a
// different error_key must not be treated as disabled.
func TestIsRuleDisabledErrorKeyMismatchDoesNotMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: disabledRulesTestCluster.ClusterName,
				RuleID:    "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey:  "SOME_OTHER_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster,
		"ccx_rules_ocp.external.rules.rule_1.report", "RULE_1_ERROR_KEY")

	assert.False(t, disabled, "same rule_id with a different error_key should not match")
}

// --- produceEntriesToKafka tests ---

// disabledRulesKafkaRuleContent is a minimal rules map with one rule whose
// total risk clears the default threshold, used by the produceEntriesToKafka
// tests below.
var disabledRulesKafkaRuleContent = types.RulesMap{
	"rule_1": {
		Summary:    "rule 1 summary",
		Resolution: "rule 1 resolution",
		MoreInfo:   "rule 1 more info",
		ErrorKeys: map[string]utypes.RuleErrorKeyContent{
			"RULE_1_ERROR_KEY": {
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

// buildDisabledRulesReportItems returns a single-item report content list
// referencing "rule_1"/"RULE_1_ERROR_KEY", matching
// disabledRulesKafkaRuleContent above.
func buildDisabledRulesReportItems() types.ReportContent {
	return types.ReportContent{
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_1.report",
				ErrorKey: "RULE_1_ERROR_KEY",
				Details:  []byte("some details"),
			},
		},
	}
}

// TestProduceEntriesToKafkaSkipsClusterDisabledRule verifies that a rule
// present in the cluster-level disabled rules map is skipped entirely: no
// message is produced to Kafka and the cluster is recorded in the "same"
// state, proving the rule never reached the total risk filter or
// ShouldNotify.
func TestProduceEntriesToKafkaSkipsClusterDisabledRule(t *testing.T) {
	storage := mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	producerMock := mocks.Producer{}

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Target:   types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: disabledRulesTestCluster.ClusterName,
				RuleID:    "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey:  "RULE_1_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{},
	}

	numEvents, err := differ.ProduceEntriesToKafka(&d, disabledRulesTestCluster,
		disabledRulesKafkaRuleContent, buildDisabledRulesReportItems(), "some report")

	assert.NoError(t, err)
	assert.Equal(t, 0, numEvents, "a rule disabled at the cluster level must not generate a notification event")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaSkipsOrgDisabledRule verifies that a rule present
// in the org-level acked rules map is skipped entirely: no message is
// produced to Kafka, proving the rule never reached the total risk filter
// or ShouldNotify.
func TestProduceEntriesToKafkaSkipsOrgDisabledRule(t *testing.T) {
	storage := mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	producerMock := mocks.Producer{}

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Target:   types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{},
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "1",
				RuleID:   "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey: "RULE_1_ERROR_KEY",
			}: {},
		},
	}

	numEvents, err := differ.ProduceEntriesToKafka(&d, disabledRulesTestCluster,
		disabledRulesKafkaRuleContent, buildDisabledRulesReportItems(), "some report")

	assert.NoError(t, err)
	assert.Equal(t, 0, numEvents, "a rule disabled at the org level must not generate a notification event")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaProcessesNonDisabledRule verifies that a rule
// absent from both the cluster-level and org-level disabled rules maps
// proceeds past the disabled check into the total risk filter and, since
// its total risk clears the threshold, it is sent to Kafka.
func TestProduceEntriesToKafkaProcessesNonDisabledRule(t *testing.T) {
	storage := mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Target:   types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{},
		OrgDisabledRules:     types.OrgDisabledRules{},
	}

	numEvents, err := differ.ProduceEntriesToKafka(&d, disabledRulesTestCluster,
		disabledRulesKafkaRuleContent, buildDisabledRulesReportItems(), "some report")

	assert.NoError(t, err)
	assert.Equal(t, 1, numEvents, "a rule not present in either disabled map should still be evaluated by the total risk filter and notified")
	producerMock.AssertCalled(t, "ProduceMessage", mock.AnythingOfType("types.ProducerMessage"))
}

// TestProduceEntriesToKafkaSkipsOnlyDisabledRuleAmongMultiple verifies that
// when a report contains both a disabled rule and a non-disabled rule, only
// the disabled rule is skipped: the notification message still contains the
// event for the non-disabled rule.
func TestProduceEntriesToKafkaSkipsOnlyDisabledRuleAmongMultiple(t *testing.T) {
	ruleContent := types.RulesMap{
		"rule_1": disabledRulesKafkaRuleContent["rule_1"],
		"rule_2": {
			Summary:    "rule 2 summary",
			Resolution: "rule 2 resolution",
			MoreInfo:   "rule 2 more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"RULE_2_ERROR_KEY": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "rule 2 error key description",
						Impact: utypes.Impact{
							Name:   "impact_2",
							Impact: 4,
						},
						Likelihood: 4,
					},
				},
			},
		},
	}

	reportItems := types.ReportContent{
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_1.report",
				ErrorKey: "RULE_1_ERROR_KEY",
				Details:  []byte("some details"),
			},
		},
		{
			ReportItem: types.ReportItem{
				Type:     "rule",
				Module:   "ccx_rules_ocp.external.rules.rule_2.report",
				ErrorKey: "RULE_2_ERROR_KEY",
				Details:  []byte("some details"),
			},
		},
	}

	storage := mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Target:   types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: disabledRulesTestCluster.ClusterName,
				RuleID:    "ccx_rules_ocp.external.rules.rule_1",
				ErrorKey:  "RULE_1_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{},
	}

	numEvents, err := differ.ProduceEntriesToKafka(&d, disabledRulesTestCluster,
		ruleContent, reportItems, "some report")

	assert.NoError(t, err)
	assert.Equal(t, 1, numEvents, "only the non-disabled rule should generate a notification event")
}
