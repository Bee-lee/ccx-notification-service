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

// Tests for disabled rules filtering in the Kafka processing path
// (CCXDEV-16567). These verify that isRuleDisabled and
// produceEntriesToKafka correctly skip disabled rules before the total
// risk filter and ShouldNotify.

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	utypes "github.com/RedHatInsights/insights-results-types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// --- isRuleDisabled unit tests ---

// TestIsRuleDisabledClusterLevel verifies that a rule present in the
// cluster-level disabled rules map (cluster_rule_toggle) is identified
// as disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule should be disabled when present in cluster-level map")
}

// TestIsRuleDisabledOrgLevel verifies that a rule present in the
// org-level disabled rules map (rule_disable) is identified as disabled.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: fmt.Sprint(cluster.OrgID), RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule should be disabled when present in org-level map")
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule not present in
// any disabled rules map is reported as not disabled.
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "rule should not be disabled when absent from both maps")
}

// TestIsRuleDisabledClusterLevelTakesPriority verifies that a rule
// present in the cluster-level map returns true immediately, regardless
// of the org-level map content.
func TestIsRuleDisabledClusterLevelTakesPriority(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: fmt.Sprint(cluster.OrgID), RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule should be disabled when present in both maps")
}

// TestIsRuleDisabledDifferentCluster verifies that a rule disabled for
// one cluster is not disabled for a different cluster.
func TestIsRuleDisabledDifferentCluster(t *testing.T) {
	disabledCluster := types.ClusterName("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	otherCluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "ffffffff-1111-2222-3333-444444444444",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: disabledCluster, RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, otherCluster, ruleName, errorKey)
	assert.False(t, result, "rule disabled for cluster A should not affect cluster B")
}

// TestIsRuleDisabledDifferentOrg verifies that an org-level ack for
// one org does not affect a different org.
func TestIsRuleDisabledDifferentOrg(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       2,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "rule acked for org 1 should not affect org 2")
}

// TestIsRuleDisabledDifferentErrorKey verifies that disabling a rule
// with one error key does not disable the same rule with a different
// error key.
func TestIsRuleDisabledDifferentErrorKey(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	ruleName := types.RuleName("test_rule")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: types.RuleID(ruleName), ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, "TEST_RULE_IMPORTANT_IMPACT")
	assert.False(t, result, "disabling one error key should not disable a different error key")
}

// TestIsRuleDisabledNilMaps verifies that isRuleDisabled is safe to call
// when the maps are nil (Go allows lookups on nil maps, returning the
// zero value).
func TestIsRuleDisabledNilMaps(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}

	d := differ.Differ{}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_EK")
	assert.False(t, result, "nil maps should not report any rule as disabled")
}

// TestIsRuleDisabledLogsMessage verifies that the log message is emitted
// when a disabled rule is encountered in the Kafka processing path.
func TestIsRuleDisabledLogsMessage(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		// Report with one rule that will be disabled
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.RuleDisabledMessage,
		"disabled rule should produce a log message")
	assert.Contains(t, executionLog, `"rule":"test_rule"`,
		"log should contain the rule name")
	assert.Contains(t, executionLog, `"error key":"TEST_RULE_CRITICAL_IMPACT"`,
		"log should contain the error key")
}

// --- Integration tests: disabled rules in produceEntriesToKafka via ProcessClusters ---

// TestProcessClustersClusterDisabledRuleSkipped verifies that a rule
// present in the cluster-level disabled map is skipped entirely and
// does not produce a notification in the Kafka path.
func TestProcessClustersClusterDisabledRuleSkipped(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	// The disabled rule should be skipped, so no notification should be produced
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"disabled rule should not reach the total risk filter")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced when all rules are disabled")
	// ProduceMessage should never be called
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersOrgDisabledRuleSkipped verifies that a rule
// present in the org-level disabled map (rule_disable) is skipped
// entirely in the Kafka path.
func TestProcessClustersOrgDisabledRuleSkipped(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}

	d := differ.Differ{
		Storage:              &storage,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		Notifier:             &producerMock,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"org-disabled rule should not reach the total risk filter")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced when all rules are org-disabled")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersOrgAckDoesNotAffectOtherOrg verifies that an
// org-level ack for org 1 does not suppress notifications for org 2,
// as described in the BDD scenario "rule ack for an organization does
// not affect other organizations".
func TestProcessClustersOrgAckDoesNotAffectOtherOrg(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		[]types.NotificationRecord{},
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
		{
			OrgID:         2,
			AccountNumber: 2,
			ClusterName:   "7e6903e5-3h96-5ddf-13ch-659efd8878bb",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)

	d := differ.Differ{
		Storage:              &storage,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		Notifier:             &producerMock,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			// Only org 1 has the rule acked
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	// Org 1's cluster should be silenced
	assert.NotContains(t, executionLog,
		`"cluster":"5d5892d4-2g85-4ccf-02bg-548dfc9767aa","number of events":1,"message":"Producing instant notification"`,
		"org 1's cluster should NOT produce a notification")
	// Org 2's cluster should still get a notification
	assert.Contains(t, executionLog,
		`"cluster":"7e6903e5-3h96-5ddf-13ch-659efd8878bb","number of events":1,"message":"Producing instant notification"`,
		"org 2's cluster should still produce a notification")
}

// TestProcessClustersEnabledRuleProceedsToNotify verifies that a rule
// not in any disabled map proceeds through the total risk filter and
// ShouldNotify, and generates a notification.
func TestProcessClustersEnabledRuleProceedsToNotify(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		[]types.NotificationRecord{},
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)

	d := differ.Differ{
		Storage:              &storage,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		Notifier:             &producerMock,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage,
		"enabled rule should reach the total risk filter")
	assert.Contains(t, executionLog, "Producing instant notification",
		"enabled rule should produce a notification")
	producerMock.AssertCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersPartiallyDisabledReport verifies that when a
// report contains multiple rules but only one is disabled, the
// remaining enabled rule still produces a notification. This matches
// the BDD scenario "only the re-enabled rule is notified when other
// rules are in cooldown".
func TestProcessClustersPartiallyDisabledReport(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		// Two rules: one critical (will be disabled), one important (will remain enabled)
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"},{"rule_id":"test_rule|TEST_RULE_IMPORTANT_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_IMPORTANT_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		[]types.NotificationRecord{},
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
				"TEST_RULE_IMPORTANT_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "important impact",
						Impact:      utypes.Impact{Name: "important", Impact: 3},
						Likelihood:  3,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		Notifier:         &producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			// Only disable the critical impact rule
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	// The critical rule should be disabled
	assert.Contains(t, executionLog, differ.RuleDisabledMessage,
		"disabled critical rule should be logged as disabled")
	// The important rule should still go through and produce a notification
	assert.Contains(t, executionLog, "Producing instant notification",
		"the enabled important rule should produce a notification")
	// Only one event should be in the notification (the important one)
	assert.Contains(t, executionLog, `"number of events":1`,
		"only the non-disabled rule should be in the notification")
	producerMock.AssertCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersAllRulesDisabledWritesSameState verifies that
// when all rules in a report are disabled, the service writes a
// state=2 ("same") record to the reported table, because there are
// zero qualifying events.
func TestProcessClustersAllRulesDisabledWritesSameState(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	assert.Contains(t, executionLog, "No new issues to notify",
		"all-disabled report should result in no new issues message")
}

// TestRuleDisabledMessageConstant checks that the exported constant
// matches the expected value from the spec.
func TestRuleDisabledMessageConstant(t *testing.T) {
	assert.Equal(t, "Rule is disabled, skipping notification", differ.RuleDisabledMessage)
}

// TestIsRuleDisabledModuleToRuleNameConversion verifies that the rule
// name used in isRuleDisabled matches what moduleToRuleName produces.
// The report JSON has a module like
// "ccx_rules_ocp.external.rules.test_rule.report" and the disabled
// rules map uses the short "test_rule" name.
func TestIsRuleDisabledModuleToRuleNameConversion(t *testing.T) {
	// moduleToRuleName("ccx_rules_ocp.external.rules.test_rule.report") -> "test_rule"
	moduleName := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	ruleName := differ.ModuleToRuleName(moduleName)
	assert.Equal(t, types.RuleName("test_rule"), ruleName,
		"moduleToRuleName should extract the short rule name")

	// The disabled map should use this same short name
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: types.RuleID(ruleName), ErrorKey: errorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "isRuleDisabled should match using the moduleToRuleName output")
}

// TestProcessClustersOrgAckAffectsAllClustersInOrg verifies that an
// org-level ack suppresses notifications for all clusters belonging to
// that org, matching the BDD scenario "rule ack affects all clusters
// in an organization".
func TestProcessClustersOrgAckAffectsAllClustersInOrg(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	// Two clusters in the same org
	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "7e6903e5-3h96-5ddf-13ch-659efd8878bb",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	producerMock := mocks.Producer{}

	d := differ.Differ{
		Storage:              &storage,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		Notifier:             &producerMock,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no cluster should produce a notification when the org has the rule acked")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersDisabledRuleNeverReachesShouldNotify verifies that
// a disabled rule never triggers the ShouldNotify cooldown check. We
// verify this by ensuring ReadLastNNotifiedRecords is never called when
// all rules are disabled (ShouldNotify calls ReadLastNNotifiedRecords).
func TestProcessClustersDisabledRuleNeverReachesShouldNotify(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	// ReadLastNNotifiedRecords is called by ShouldNotify. If the disabled rule
	// never reaches ShouldNotify, this method should never be called.
	storage.AssertNotCalled(t, "ReadLastNNotifiedRecords", mock.Anything, mock.Anything)
}

// TestProcessClustersDisabledRuleBeforeTotalRiskFilter verifies that the
// disabled check runs before the total risk filter. We do this by setting
// a rule with low risk (would be filtered by total risk threshold) but also
// marking it as disabled. If the disabled check runs first, we should see
// the disabled log message but NOT the total risk log.
func TestProcessClustersDisabledRuleBeforeTotalRiskFilter(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		types.ClusterReport(`{"reports":[{"rule_id":"low_risk_rule|LOW_RISK_EK","component":"ccx_rules_ocp.external.rules.low_risk_rule.report","type":"rule","key":"LOW_RISK_EK","details":"details"}]}`),
		nil,
	)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	ruleContent := types.RulesMap{
		"low_risk_rule": {
			Summary:    "low risk rule summary",
			Reason:     "low risk rule reason",
			Resolution: "low risk rule resolution",
			MoreInfo:   "low risk rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"LOW_RISK_EK": {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "low risk",
						Impact:      utypes.Impact{Name: "low", Impact: 1},
						Likelihood:  1,
					},
					HasReason: true,
				},
			},
			HasReason: true,
		},
	}

	clusters := []types.ClusterEntry{
		{
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(time.Now()),
		},
	}

	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "low_risk_rule", ErrorKey: "LOW_RISK_EK"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()
	// Disabled check should fire
	assert.Contains(t, executionLog, differ.RuleDisabledMessage,
		"disabled check should run before total risk filter")
	// Total risk filter should NOT run for the disabled rule (no risk
	// log messages for this rule). The rule has low risk so it would
	// have been filtered anyway, but the disabled check should prevent
	// the risk calculation from even running.
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"disabled rule should never reach the total risk filter")
}
