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

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	utypes "github.com/RedHatInsights/insights-results-types"

	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// --- isRuleDisabled unit tests ---

// TestIsRuleDisabledClusterLevel verifies that a rule present in the
// cluster-level disabled rules map (cluster_rule_toggle) is reported
// as disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule in cluster-level disabled map should be reported as disabled")
}

// TestIsRuleDisabledOrgLevel verifies that a rule present in the org-level
// disabled rules map (rule_disable) is reported as disabled.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule in org-level disabled map should be reported as disabled")
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule not present in
// either disabled rules map is reported as not disabled.
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "rule not in any disabled map should not be reported as disabled")
}

// TestIsRuleDisabledNilMaps verifies that isRuleDisabled handles nil maps
// gracefully and returns false.
func TestIsRuleDisabledNilMaps(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "some-cluster",
	}

	d := differ.Differ{}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE")
	assert.False(t, result, "nil maps should be handled safely and return false")
}

// TestIsRuleDisabledInBothMaps verifies that when a rule is present in both
// the cluster-level and org-level disabled maps, it is still reported as
// disabled (short-circuits on the first check).
func TestIsRuleDisabledInBothMaps(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.True(t, result, "rule in both disabled maps should be reported as disabled")
}

// TestIsRuleDisabledDifferentClusterNotMatched verifies that a cluster-level
// disabled rule for one cluster does not affect a different cluster.
func TestIsRuleDisabledDifferentClusterNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "different-cluster-uuid",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "cluster-level disabled rule should not match a different cluster")
}

// TestIsRuleDisabledDifferentOrgNotMatched verifies that an org-level
// disabled rule for one organization does not affect a different organization.
func TestIsRuleDisabledDifferentOrgNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       2,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "org-level disabled rule should not match a different org")
}

// TestIsRuleDisabledDifferentErrorKeyNotMatched verifies that a disabled
// rule with a different error key does not match.
func TestIsRuleDisabledDifferentErrorKeyNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	}
	ruleName := types.RuleName("test_rule")
	errorKey := types.ErrorKey("TEST_RULE_IMPORTANT_IMPACT")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, ruleName, errorKey)
	assert.False(t, result, "disabled rule with a different error key should not match")
}

// TestIsRuleDisabledOrgKeyUsesOrgIDFromCluster verifies that isRuleDisabled
// uses the cluster's OrgID (converted via fmt.Sprint) for the org-level
// lookup, matching the format stored in the rule_disable table.
func TestIsRuleDisabledOrgKeyUsesOrgIDFromCluster(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       42,
		ClusterName: "any-cluster",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "42", RuleID: "test_rule", ErrorKey: "EK1"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "EK1")
	assert.True(t, result, "org-level lookup should match OrgID 42 formatted as string \"42\"")
}

// --- Integration tests: disabled rules in the Kafka processing path ---

// TestProcessClustersDisabledRuleSkippedClusterLevel verifies that when a
// rule is in the cluster-level disabled map, it is skipped in the Kafka
// path and no notification is produced for it. This corresponds to the BDD
// scenario "Check that a rule disabled for a single cluster affects the
// cluster".
func TestProcessClustersDisabledRuleSkippedClusterLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

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

	// The rule should have been skipped due to the cluster-level disabled check
	assert.Contains(t, executionLog, "Rule is disabled, skipping notification",
		"disabled rule should produce the skip log message")

	// No notification should have been produced
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"disabled rule should not reach the total risk filter")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced when the only rule is disabled")

	// The "same state" record should be written (0 events)
	assert.Contains(t, executionLog, "No new issues to notify",
		"cluster with all rules disabled should report no new issues")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

// TestProcessClustersDisabledRuleSkippedOrgLevel verifies that when a rule
// is in the org-level disabled map, it is skipped in the Kafka path and
// no notification is produced for it. This corresponds to the BDD scenario
// "Check that a rule ack affects all clusters in an organization".
func TestProcessClustersDisabledRuleSkippedOrgLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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
			OrgID:         1,
			AccountNumber: 1,
			ClusterName:   "7e6903e5-3h96-5ddf-13ch-659efd8878bb",
			KafkaOffset:   0,
			UpdatedAt:     types.Timestamp(testTimestamp),
		},
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

	d := differ.Differ{
		Storage:              &storage,
		NotificationType:     types.InstantNotif,
		Target:               types.NotificationBackendTarget,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()

	// Both clusters should have the rule skipped
	assert.Contains(t, executionLog, "Rule is disabled, skipping notification",
		"org-level disabled rule should produce the skip log message")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"org-level disabled rule should not reach the total risk filter")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced when the only rule is org-level disabled")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

// TestProcessClustersOrgDisabledRuleDoesNotAffectOtherOrgs verifies that an
// org-level disabled rule for org 1 does not prevent org 2 from receiving
// notifications. This corresponds to the BDD scenario "Check that a rule ack
// for an organization does not affect other organizations".
func TestProcessClustersOrgDisabledRuleDoesNotAffectOtherOrgs(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		func(clusterEntry types.ClusterEntry, numberOfRecords int) []types.NotificationRecord {
			return []types.NotificationRecord{}
		},
		func(clusterEntry types.ClusterEntry, numberOfRecords int) error {
			return nil
		},
	)

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

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
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()

	// Org 1's cluster should have the rule skipped
	assert.Contains(t, executionLog, "Rule is disabled, skipping notification",
		"org 1's cluster should have the rule skipped")

	// Org 2's cluster should still get a notification
	assert.Contains(t, executionLog, "Producing instant notification",
		"org 2's cluster should still produce a notification")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

// TestProcessClustersNonDisabledRuleProceedsNormally verifies that when
// disabled rule maps are populated but do not contain the current rule,
// the rule proceeds through the total risk filter and ShouldNotify as
// normal.
func TestProcessClustersNonDisabledRuleProceedsNormally(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		func(clusterEntry types.ClusterEntry, numberOfRecords int) []types.NotificationRecord {
			return []types.NotificationRecord{}
		},
		func(clusterEntry types.ClusterEntry, numberOfRecords int) error {
			return nil
		},
	)

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)

	// Disabled maps contain a DIFFERENT rule, not the one in the report
	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		Notifier:         &producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "other_rule", ErrorKey: "OTHER_ERROR_KEY"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "yet_another_rule", ErrorKey: "YET_ANOTHER_KEY"}: {},
		},
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()

	// The rule should NOT have been skipped
	assert.NotContains(t, executionLog, "Rule is disabled, skipping notification",
		"non-disabled rule should not be skipped")

	// The rule should have reached the total risk filter and produced a notification
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage,
		"non-disabled rule should reach the total risk filter")
	assert.Contains(t, executionLog, "Producing instant notification",
		"non-disabled rule should result in a notification being produced")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

// TestProcessClustersDisabledRuleNeverReachesShouldNotify verifies that a
// disabled rule is skipped before reaching ShouldNotify. If ShouldNotify
// were called, it would invoke ReadLastNNotifiedRecords on the storage
// mock. By not setting up that mock expectation and verifying the test
// passes, we confirm the disabled check prevents ShouldNotify from running.
func TestProcessClustersDisabledRuleNeverReachesShouldNotify(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	// Deliberately NOT setting up ReadLastNNotifiedRecords — if
	// produceEntriesToKafka calls ShouldNotify for the disabled rule,
	// the mock would cause a test failure because no expectation was set.

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

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
	assert.Contains(t, executionLog, "Rule is disabled, skipping notification")

	// Verify that ReadLastNNotifiedRecords was never called (ShouldNotify
	// was never reached).
	storage.AssertNotCalled(t, "ReadLastNNotifiedRecords", mock.Anything, mock.Anything)

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

// TestProcessClustersDisabledRuleSkipsOnlyMatchingRule verifies that when
// multiple rules are in a report and only one is disabled, only the disabled
// rule is skipped. The non-disabled rule should still produce a notification.
// This corresponds to the BDD scenario "Check that only the re-enabled rule
// is notified when other rules are in cooldown" (first run portion).
func TestProcessClustersDisabledRuleSkipsOnlyMatchingRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact: utypes.Impact{
					Name:   "Critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
		},
		"TEST_RULE_IMPORTANT_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule important impact",
				Impact: utypes.Impact{
					Name:   "Important",
					Impact: 3,
				},
				Likelihood: 3,
			},
		},
	}

	ruleContent := types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
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

	// Report has two rules: critical (disabled) and important (not disabled)
	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) types.ClusterReport {
			return "{\"analysis_metadata\":{\"metadata\":\"some metadata\"},\"reports\":[{\"rule_id\":\"test_rule|TEST_RULE_CRITICAL_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_CRITICAL_IMPACT\",\"details\":\"some details\"},{\"rule_id\":\"test_rule|TEST_RULE_IMPORTANT_IMPACT\",\"component\":\"ccx_rules_ocp.external.rules.test_rule.report\",\"type\":\"rule\",\"key\":\"TEST_RULE_IMPORTANT_IMPACT\",\"details\":\"some details\"}]}"
		},
		func(orgID types.OrgID, clusterName types.ClusterName, updatedAt types.Timestamp) error {
			return nil
		},
	)

	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return(
		func(clusterEntry types.ClusterEntry, numberOfRecords int) []types.NotificationRecord {
			return []types.NotificationRecord{}
		},
		func(clusterEntry types.ClusterEntry, numberOfRecords int) error {
			return nil
		},
	)

	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(
		func(clusterEntry types.ClusterEntry, notificationTypeID types.NotificationTypeID, stateID types.StateID, report types.ClusterReport, notifiedAt types.Timestamp, errorLog string, eventTarget types.EventTarget) error {
			return nil
		},
	)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil,
	)

	// Only the critical rule is disabled
	d := differ.Differ{
		Storage:          &storage,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds:       differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:           differ.DefaultEventFilter,
		Notifier:         &producerMock,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-2g85-4ccf-02bg-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}
	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}}, ruleContent, clusters)

	executionLog := buf.String()

	// The critical rule should be skipped
	assert.Contains(t, executionLog, "Rule is disabled, skipping notification",
		"disabled critical rule should be skipped")

	// The important (non-disabled) rule should still produce a notification
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage,
		"non-disabled important rule should pass the total risk filter")
	assert.Contains(t, executionLog, "Producing instant notification",
		"a notification should be produced for the non-disabled rule")
	assert.Contains(t, executionLog, "\"number of events\":1",
		"exactly 1 event should be in the notification (the non-disabled rule)")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}
