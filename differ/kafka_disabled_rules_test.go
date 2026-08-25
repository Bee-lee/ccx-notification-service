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

// This file covers CCXDEV-16567: filtering of disabled rules in the Kafka
// notification path (produceEntriesToKafka / isRuleDisabled).

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"

	utypes "github.com/RedHatInsights/insights-results-types"
)

// --- isRuleDisabled unit tests ---

// disabledRulesTestCluster is the cluster entry shared by the isRuleDisabled
// test cases below.
var disabledRulesTestCluster = types.ClusterEntry{
	OrgID:       types.OrgID(1),
	ClusterName: "cluster-1",
}

// TestIsRuleDisabledClusterLevelMatch verifies a rule present in the
// cluster-level (cluster_rule_toggle) map is reported as disabled.
func TestIsRuleDisabledClusterLevelMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster, "rule_1", "RULE_1")
	assert.True(t, disabled, "rule present in cluster-level map should be disabled")
}

// TestIsRuleDisabledOrgLevelMatch verifies a rule present in the org-level
// (rule_disable) map is reported as disabled.
func TestIsRuleDisabledOrgLevelMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster, "rule_1", "RULE_1")
	assert.True(t, disabled, "rule present in org-level map should be disabled")
}

// TestIsRuleDisabledNoMatch verifies that a rule absent from both maps is not
// considered disabled.
func TestIsRuleDisabledNoMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "other-cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "2", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster, "rule_1", "RULE_1")
	assert.False(t, disabled, "rule not matching cluster or org key should not be disabled")
}

// TestIsRuleDisabledDifferentErrorKeyNoMatch verifies that matching
// cluster/rule but a different error key is not treated as disabled.
func TestIsRuleDisabledDifferentErrorKeyNoMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
	}

	disabled := differ.IsRuleDisabled(&d, disabledRulesTestCluster, "rule_1", "RULE_2")
	assert.False(t, disabled, "same rule but different error key should not be disabled")
}

// --- produceEntriesToKafka integration tests (disabled rules filtering) ---

func newKafkaDisabledRulesRuleContent() types.RulesMap {
	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"RULE_1": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "rule 1 error key description",
				Impact: utypes.Impact{
					Name:   "impact_1",
					Impact: 4,
				},
				Likelihood: 4,
			},
			Reason:    "rule 1 reason",
			HasReason: true,
		},
		"RULE_2": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "rule 2 error key description",
				Impact: utypes.Impact{
					Name:   "impact_2",
					Impact: 4,
				},
				Likelihood: 4,
			},
			Reason:    "rule 2 reason",
			HasReason: true,
		},
	}

	return types.RulesMap{
		"rule_1": {
			Summary:    "rule 1 summary",
			Reason:     "rule 1 reason",
			Resolution: "rule 1 resolution",
			MoreInfo:   "rule 1 more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
		},
		"rule_2": {
			Summary:    "rule 2 summary",
			Reason:     "rule 2 reason",
			Resolution: "rule 2 resolution",
			MoreInfo:   "rule 2 more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
		},
	}
}

func newKafkaDisabledRulesReportItem(module types.ModuleName, errorKey types.ErrorKey) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			Module:   module,
			ErrorKey: errorKey,
		},
	}
}

func newKafkaDisabledRulesStorageMock() *mocks.Storage {
	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)
	return storage
}

func newKafkaDisabledRulesProducerMock() *mocks.Producer {
	producerMock := &mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(1), nil,
	)
	return producerMock
}

// TestProduceEntriesToKafkaSkipsClusterLevelDisabledRule verifies that a rule
// present in the cluster-level disabled rules map is skipped entirely: no
// message is produced to Kafka and the rule never reaches the total risk
// filter or ShouldNotify.
func TestProduceEntriesToKafkaSkipsClusterLevelDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	cluster := types.ClusterEntry{
		OrgID:       types.OrgID(1),
		ClusterName: "cluster-1",
	}

	storage := newKafkaDisabledRulesStorageMock()
	producerMock := newKafkaDisabledRulesProducerMock()

	d := differ.Differ{
		Storage: storage,
		Target:  types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:   differ.DefaultEventFilter,
		Notifier: producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules:   make(types.OrgDisabledRules),
		PreviouslyReported: make(types.NotifiedRecordsPerCluster),
	}

	reportItems := types.ReportContent{
		newKafkaDisabledRulesReportItem("ccx_rules_ocp.external.rules.rule_1.report", "RULE_1"),
	}

	totalMessages, err := differ.ProduceEntriesToKafka(&d, cluster, newKafkaDisabledRulesRuleContent(), reportItems, "some report")

	assert.NoError(t, err)
	assert.Equal(t, 0, totalMessages, "a disabled rule must not produce a notification")
	executionLog := buf.String()
	assert.Contains(t, executionLog, "Rule is disabled, skipping", "expected a debug log confirming the rule was skipped as disabled")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage, "a disabled rule must never reach the total risk filter")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaSkipsOrgLevelDisabledRule verifies that a rule
// present in the org-level (acked) disabled rules map is skipped entirely.
func TestProduceEntriesToKafkaSkipsOrgLevelDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	cluster := types.ClusterEntry{
		OrgID:       types.OrgID(1),
		ClusterName: "cluster-1",
	}

	storage := newKafkaDisabledRulesStorageMock()
	producerMock := newKafkaDisabledRulesProducerMock()

	d := differ.Differ{
		Storage: storage,
		Target:  types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:               differ.DefaultEventFilter,
		Notifier:             producerMock,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		PreviouslyReported: make(types.NotifiedRecordsPerCluster),
	}

	reportItems := types.ReportContent{
		newKafkaDisabledRulesReportItem("ccx_rules_ocp.external.rules.rule_1.report", "RULE_1"),
	}

	totalMessages, err := differ.ProduceEntriesToKafka(&d, cluster, newKafkaDisabledRulesRuleContent(), reportItems, "some report")

	assert.NoError(t, err)
	assert.Equal(t, 0, totalMessages, "an org-acked disabled rule must not produce a notification")
	executionLog := buf.String()
	assert.Contains(t, executionLog, "Rule is disabled, skipping", "expected a debug log confirming the rule was skipped as disabled")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage, "a disabled rule must never reach the total risk filter")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaRuleNotDisabledProceedsToFilter verifies that a
// rule not present in either disabled rules map proceeds normally through
// the total risk filter and gets notified.
func TestProduceEntriesToKafkaRuleNotDisabledProceedsToFilter(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	cluster := types.ClusterEntry{
		OrgID:       types.OrgID(1),
		ClusterName: "cluster-1",
	}

	storage := newKafkaDisabledRulesStorageMock()
	producerMock := newKafkaDisabledRulesProducerMock()

	d := differ.Differ{
		Storage: storage,
		Target:  types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:   differ.DefaultEventFilter,
		Notifier: producerMock,
		// Neither map has an entry matching this cluster/rule/error key.
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "other-cluster", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "2", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		PreviouslyReported: make(types.NotifiedRecordsPerCluster),
	}

	reportItems := types.ReportContent{
		newKafkaDisabledRulesReportItem("ccx_rules_ocp.external.rules.rule_1.report", "RULE_1"),
	}

	totalMessages, err := differ.ProduceEntriesToKafka(&d, cluster, newKafkaDisabledRulesRuleContent(), reportItems, "some report")

	assert.NoError(t, err)
	assert.Equal(t, 1, totalMessages, "a rule that is not disabled must be evaluated and notified")
	executionLog := buf.String()
	assert.NotContains(t, executionLog, "Rule is disabled, skipping")
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage, "a non-disabled rule must reach the total risk filter")
	producerMock.AssertCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaMixOfDisabledAndEnabledRules verifies that when a
// report contains both a disabled and a non-disabled rule, only the
// non-disabled rule is notified, confirming the disabled rule check happens
// per-rule rather than short-circuiting the whole cluster.
func TestProduceEntriesToKafkaMixOfDisabledAndEnabledRules(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	cluster := types.ClusterEntry{
		OrgID:       types.OrgID(1),
		ClusterName: "cluster-1",
	}

	storage := newKafkaDisabledRulesStorageMock()
	producerMock := newKafkaDisabledRulesProducerMock()

	d := differ.Differ{
		Storage: storage,
		Target:  types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:   differ.DefaultEventFilter,
		Notifier: producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: "rule_1", ErrorKey: "RULE_1"}: {},
		},
		OrgDisabledRules:   make(types.OrgDisabledRules),
		PreviouslyReported: make(types.NotifiedRecordsPerCluster),
	}

	reportItems := types.ReportContent{
		newKafkaDisabledRulesReportItem("ccx_rules_ocp.external.rules.rule_1.report", "RULE_1"),
		newKafkaDisabledRulesReportItem("ccx_rules_ocp.external.rules.rule_2.report", "RULE_2"),
	}

	totalMessages, err := differ.ProduceEntriesToKafka(&d, cluster, newKafkaDisabledRulesRuleContent(), reportItems, "some report")

	assert.NoError(t, err)
	assert.Equal(t, 1, totalMessages, "only the non-disabled rule should be notified")
	executionLog := buf.String()
	assert.Contains(t, executionLog, "Rule is disabled, skipping")
	assert.Contains(t, executionLog, `"rule":"rule_2"`, "the non-disabled rule should reach the total risk filter")
}
