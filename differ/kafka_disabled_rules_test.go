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

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	utypes "github.com/RedHatInsights/insights-results-types"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// This file covers CCXDEV-16567: filtering of disabled rules in the Kafka
// processing path (produceEntriesToKafka), plus direct tests of the shared
// isRuleDisabled helper it relies on.

// riskRuleContent builds a minimal types.RulesMap holding a single rule name
// / error key pair with the given likelihood and impact, so that
// findRuleByNameAndErrorKey resolves a specific total risk
// ((likelihood+impact)/2) for the tests below.
func riskRuleContent(ruleName, errorKey string, likelihood, impact int) types.RulesMap {
	return types.RulesMap{
		ruleName: utypes.RuleContent{
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				errorKey: {
					Metadata: utypes.ErrorKeyMetadata{
						Likelihood: likelihood,
						Impact:     utypes.Impact{Impact: impact},
					},
				},
			},
		},
	}
}

// highRiskRuleContent returns rule content whose total risk (4) is well
// above differ.DefaultTotalRiskThreshold (2), so the rule would pass the
// total risk filter (and, since PreviouslyReported is left empty, would also
// pass ShouldNotify) if it were not skipped for being disabled.
func highRiskRuleContent(ruleName, errorKey string) types.RulesMap {
	return riskRuleContent(ruleName, errorKey, 4, 4)
}

// reportItem builds a single report entry for the given fully qualified
// module and error key, matching the shape produced when a report is
// deserialized from new_reports.
func reportItem(module, errorKey string) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			Module:   types.ModuleName(module),
			ErrorKey: types.ErrorKey(errorKey),
		},
	}
}

// expectWriteNotificationRecord sets up the storage mock to accept a single
// call to WriteNotificationRecordForCluster regardless of the resulting
// state (same/sent/error), which is all that matters for these tests.
func expectWriteNotificationRecord(storage *mocks.Storage) {
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)
}

// --- produceEntriesToKafka tests (CCXDEV-16567) ---

// TestProduceEntriesToKafkaSkipsRuleDisabledAtClusterLevel verifies that a
// rule present in the per-cluster disabled rules map (cluster_rule_toggle)
// is skipped entirely: even though its total risk is well above the
// threshold and there is no previous notification record that would put it
// in cooldown (so both the total risk filter and ShouldNotify would
// otherwise let it through), no notification is produced.
func TestProduceEntriesToKafkaSkipsRuleDisabledAtClusterLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "cluster_1",
		UpdatedAt:   types.Timestamp(testTimestamp),
	}
	reportItems := types.ReportContent{
		reportItem("ccx_rules_ocp.external.rules.test_rule.report", "TEST_RULE_CRITICAL_IMPACT"),
	}
	ruleContent := highRiskRuleContent("test_rule", "TEST_RULE_CRITICAL_IMPACT")

	storage := mocks.Storage{}
	expectWriteNotificationRecord(&storage)

	// Registered only as a safety net: if produceEntriesToKafka incorrectly
	// reaches the producer, this stub prevents a mock panic so the
	// AssertNotCalled check below can report a clean failure instead.
	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(0), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	n, err := differ.ProduceEntriesToKafka(&d, cluster, ruleContent, reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, n, "a cluster-level disabled rule must not be counted as a notified issue")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.RuleDisabledMessage)
	assert.Contains(t, executionLog, "test_rule")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"a disabled rule must never reach the total risk filter or ShouldNotify")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
	storage.AssertExpectations(t)
}

// TestProduceEntriesToKafkaSkipsRuleDisabledAtOrgLevel verifies that a rule
// present in the org-wide disabled rules map (rule_disable) is skipped
// entirely, with the same guarantees as the per-cluster case above.
func TestProduceEntriesToKafkaSkipsRuleDisabledAtOrgLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "cluster_1",
		UpdatedAt:   types.Timestamp(testTimestamp),
	}
	reportItems := types.ReportContent{
		reportItem("ccx_rules_ocp.external.rules.test_rule.report", "TEST_RULE_CRITICAL_IMPACT"),
	}
	ruleContent := highRiskRuleContent("test_rule", "TEST_RULE_CRITICAL_IMPACT")

	storage := mocks.Storage{}
	expectWriteNotificationRecord(&storage)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(0), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: fmt.Sprint(cluster.OrgID), RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	n, err := differ.ProduceEntriesToKafka(&d, cluster, ruleContent, reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, n, "an org-level disabled (acked) rule must not be counted as a notified issue")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.RuleDisabledMessage)
	assert.Contains(t, executionLog, "test_rule")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"a disabled rule must never reach the total risk filter or ShouldNotify")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
	storage.AssertExpectations(t)
}

// TestProduceEntriesToKafkaNotDisabledRuleProceedsToTotalRiskFilter verifies
// that a rule absent from both disabled-rule maps is not skipped by the
// disabled check: it proceeds to the total risk filter, where it is
// filtered out on its own merits (total risk below threshold). The absence
// of the disabled-rule log message distinguishes this from being skipped as
// disabled.
func TestProduceEntriesToKafkaNotDisabledRuleProceedsToTotalRiskFilter(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "cluster_1",
		UpdatedAt:   types.Timestamp(testTimestamp),
	}
	reportItems := types.ReportContent{
		reportItem("ccx_rules_ocp.external.rules.test_rule.report", "TEST_RULE_CRITICAL_IMPACT"),
	}
	// likelihood=1, impact=1 -> total risk 1, below DefaultTotalRiskThreshold (2)
	ruleContent := riskRuleContent("test_rule", "TEST_RULE_CRITICAL_IMPACT", 1, 1)

	storage := mocks.Storage{}
	expectWriteNotificationRecord(&storage)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(0), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
	}

	n, err := differ.ProduceEntriesToKafka(&d, cluster, ruleContent, reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, n, "a rule below the total risk threshold must not be counted as a notified issue")

	executionLog := buf.String()
	assert.NotContains(t, executionLog, differ.RuleDisabledMessage,
		"a rule absent from both disabled-rule maps must not be skipped as disabled")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
	storage.AssertExpectations(t)
}

// TestProduceEntriesToKafkaOnlyNonDisabledRuleIsNotified exercises
// per-rule granularity within a single report: one rule is disabled at the
// cluster level, a second rule is not disabled and clears the total risk
// filter. Only the second rule should result in a notification event, and
// the Kafka producer should be invoked exactly once for the cluster.
func TestProduceEntriesToKafkaOnlyNonDisabledRuleIsNotified(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "cluster_1",
		UpdatedAt:   types.Timestamp(testTimestamp),
	}
	reportItems := types.ReportContent{
		reportItem("ccx_rules_ocp.external.rules.test_rule.report", "TEST_RULE_CRITICAL_IMPACT"),
		reportItem("ccx_rules_ocp.external.rules.other_rule.report", "OTHER_RULE_ERROR_KEY"),
	}
	ruleContent := types.RulesMap{
		"test_rule": utypes.RuleContent{
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"TEST_RULE_CRITICAL_IMPACT": {
					Metadata: utypes.ErrorKeyMetadata{Likelihood: 4, Impact: utypes.Impact{Impact: 4}},
				},
			},
		},
		"other_rule": utypes.RuleContent{
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				"OTHER_RULE_ERROR_KEY": {
					Metadata: utypes.ErrorKeyMetadata{Likelihood: 4, Impact: utypes.Impact{Impact: 4}},
				},
			},
		},
	}

	storage := mocks.Storage{}
	expectWriteNotificationRecord(&storage)

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(0), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter: differ.DefaultEventFilter,
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: cluster.ClusterName, RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	n, err := differ.ProduceEntriesToKafka(&d, cluster, ruleContent, reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 1, n, "only the non-disabled rule should be counted as a notified issue")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.RuleDisabledMessage)
	assert.Contains(t, executionLog, "test_rule")
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage)
	assert.Contains(t, executionLog, "other_rule")

	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
	storage.AssertExpectations(t)
}

// --- isRuleDisabled tests (CCXDEV-16567) ---

// TestIsRuleDisabledClusterLevelMatch verifies that a rule present in the
// per-cluster disabled rules map (keyed by cluster/rule_id/error_key) is
// reported as disabled.
func TestIsRuleDisabledClusterLevelMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster_1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster_1"}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT"))
}

// TestIsRuleDisabledOrgLevelMatch verifies that a rule present in the
// org-wide disabled rules map (keyed by org/rule_id/error_key) is reported
// as disabled, even with no per-cluster entry at all.
func TestIsRuleDisabledOrgLevelMatch(t *testing.T) {
	d := differ.Differ{
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster_1"}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT"))
}

// TestIsRuleDisabledBothMapsMatch verifies that either match is sufficient:
// a rule disabled in both the cluster-level and org-level maps at once is
// still reported as disabled (no special-casing of the "both" case).
func TestIsRuleDisabledBothMapsMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster_1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster_1"}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT"))
}

// TestIsRuleDisabledNoMatch verifies that entries scoped to a different
// cluster or a different org do not disable the rule for this cluster/org:
// matching is scoped by identity, not just rule_id/error_key.
func TestIsRuleDisabledNoMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "another_cluster", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "2", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster_1"}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT"))
}

// TestIsRuleDisabledErrorKeyMismatch verifies that the error key is part of
// the composite matching key: the same rule_id disabled under a different
// error key does not disable this error key.
func TestIsRuleDisabledErrorKeyMismatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster_1", RuleID: "test_rule", ErrorKey: "TEST_RULE_IMPORTANT_IMPACT"}: {},
		},
	}
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster_1"}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT"))
}
