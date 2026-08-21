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

// Tests for the disabled-rules filtering added to the Kafka processing path
// (CCXDEV-16567). They cover both the isRuleDisabled helper and its use inside
// produceEntriesToKafka, asserting that a disabled rule (per-cluster or org-wide)
// is skipped before it ever reaches the total risk filter or ShouldNotify.

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

const (
	// disabledRuleModule is the fully qualified module name as it appears in the
	// report JSON reports[].component field.
	disabledRuleModule = "ccx_rules_ocp.external.rules.test_rule.report"
	// disabledRuleShortName is what moduleToRuleName derives from the module and
	// what the aggregator DB stores in the rule_id column.
	disabledRuleShortName = "test_rule"
	disabledRuleErrorKey  = "TEST_RULE_CRITICAL_IMPACT"
	// shouldNotifyMessage is the log message emitted by ShouldNotify. Its absence
	// proves a rule never reached the ShouldNotify step.
	shouldNotifyMessage = "Should notify user"
)

// buildCriticalRuleContent returns rule content that resolves to a critical
// total risk (impact 4, likelihood 4 => totalRisk 4) so the rule would pass the
// default total risk filter if it were not skipped as disabled.
func buildCriticalRuleContent() types.RulesMap {
	return buildRuleContentWith(4, 4)
}

// buildRuleContentWith builds a RulesMap for the test rule/error key with the
// given impact and likelihood values.
func buildRuleContentWith(impact, likelihood int) types.RulesMap {
	return types.RulesMap{
		disabledRuleShortName: {
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				disabledRuleErrorKey: {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "test rule error key description",
						Impact: utypes.Impact{
							Name:   "impact",
							Impact: impact,
						},
						Likelihood: likelihood,
					},
				},
			},
		},
	}
}

// buildReportItem builds an evaluated report item using the fully qualified
// module name, mirroring what is deserialized from the report JSON.
func buildReportItem(module, errorKey string) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			Module:   types.ModuleName(module),
			ErrorKey: types.ErrorKey(errorKey),
		},
	}
}

// --- isRuleDisabled unit tests -------------------------------------------------

// TestIsRuleDisabledClusterLevelMatch verifies that a rule listed in the
// per-cluster cluster_rule_toggle map is reported as disabled. The map is keyed
// by the short rule name (as stored in the aggregator DB) while the report item
// carries the fully qualified module name, so this also proves the composite
// rule_id|error_key parsing derives the correct rule_id for matching.
func TestIsRuleDisabledClusterLevelMatch(t *testing.T) {
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster-1"}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	reportItem := buildReportItem(disabledRuleModule, disabledRuleErrorKey)
	assert.True(t, differ.IsRuleDisabled(&d, cluster, reportItem))
}

// TestIsRuleDisabledOrgLevelMatch verifies that a rule acked org-wide (present in
// the rule_disable map keyed by org_id/rule_id/error_key) is reported as
// disabled. The org ID is matched as its string representation.
func TestIsRuleDisabledOrgLevelMatch(t *testing.T) {
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster-1"}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
	}

	reportItem := buildReportItem(disabledRuleModule, disabledRuleErrorKey)
	assert.True(t, differ.IsRuleDisabled(&d, cluster, reportItem))
}

// TestIsRuleDisabledNotDisabled verifies that a rule absent from both maps is not
// reported as disabled.
func TestIsRuleDisabledNotDisabled(t *testing.T) {
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster-1"}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	reportItem := buildReportItem(disabledRuleModule, disabledRuleErrorKey)
	assert.False(t, differ.IsRuleDisabled(&d, cluster, reportItem))
}

// TestIsRuleDisabledClusterLevelDoesNotAffectOtherCluster verifies that a rule
// disabled for one cluster does not count as disabled for a different cluster
// (per-cluster specificity, see BDD "rule disabled for a single cluster").
func TestIsRuleDisabledClusterLevelDoesNotAffectOtherCluster(t *testing.T) {
	otherCluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster-2"}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	reportItem := buildReportItem(disabledRuleModule, disabledRuleErrorKey)
	assert.False(t, differ.IsRuleDisabled(&d, otherCluster, reportItem))
}

// TestIsRuleDisabledOrgAckDoesNotAffectOtherOrg verifies that an org-wide ack for
// one organization does not count as disabled for a cluster in another
// organization (see BDD "rule ack for an organization does not affect other
// organizations").
func TestIsRuleDisabledOrgAckDoesNotAffectOtherOrg(t *testing.T) {
	clusterOtherOrg := types.ClusterEntry{OrgID: 2, ClusterName: "cluster-2"}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
	}

	reportItem := buildReportItem(disabledRuleModule, disabledRuleErrorKey)
	assert.False(t, differ.IsRuleDisabled(&d, clusterOtherOrg, reportItem))
}

// TestIsRuleDisabledDifferentErrorKeyNotMatched verifies that the error key is
// part of the match: a different error key on the same rule is not disabled.
func TestIsRuleDisabledDifferentErrorKeyNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{OrgID: 1, ClusterName: "cluster-1"}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	reportItem := buildReportItem(disabledRuleModule, "TEST_RULE_IMPORTANT_IMPACT")
	assert.False(t, differ.IsRuleDisabled(&d, cluster, reportItem))
}

// --- produceEntriesToKafka disabled-filtering tests ---------------------------

// TestProduceEntriesToKafkaSkipsClusterDisabledRule verifies that a rule disabled
// per-cluster is skipped entirely: no Kafka message is produced, no event is
// counted, and the rule never reaches ShouldNotify. The rule content resolves to
// a critical total risk, so absent the disabled check it would be notified.
func TestProduceEntriesToKafkaSkipsClusterDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "cluster-1",
		UpdatedAt:     types.Timestamp(testTimestamp),
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

	d := differ.Differ{
		Storage:  &storage,
		Target:   types.NotificationBackendTarget,
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	reportItems := types.ReportContent{buildReportItem(disabledRuleModule, disabledRuleErrorKey)}
	sent, err := differ.ProduceEntriesToKafka(&d, cluster, buildCriticalRuleContent(), reportItems, "{}")

	assert.NoError(t, err)
	assert.Equal(t, 0, sent, "a disabled rule must not produce any notification event")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.DisabledRuleSkippedMessage, "the disabled-rule skip should be logged")
	assert.NotContains(t, executionLog, shouldNotifyMessage, "a disabled rule must not reach ShouldNotify")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage, "a disabled rule must not reach the notification stage")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaSkipsOrgDisabledRule verifies that a rule acked
// org-wide is skipped entirely in the Kafka path, exactly like a per-cluster
// disabled rule.
func TestProduceEntriesToKafkaSkipsOrgDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "cluster-1",
		UpdatedAt:     types.Timestamp(testTimestamp),
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

	d := differ.Differ{
		Storage:  &storage,
		Target:   types.NotificationBackendTarget,
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
	}

	reportItems := types.ReportContent{buildReportItem(disabledRuleModule, disabledRuleErrorKey)}
	sent, err := differ.ProduceEntriesToKafka(&d, cluster, buildCriticalRuleContent(), reportItems, "{}")

	assert.NoError(t, err)
	assert.Equal(t, 0, sent, "an org-wide acked rule must not produce any notification event")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.DisabledRuleSkippedMessage, "the disabled-rule skip should be logged")
	assert.NotContains(t, executionLog, shouldNotifyMessage, "a disabled rule must not reach ShouldNotify")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaNotifiesRuleNotDisabled verifies that a rule absent
// from both disabled maps proceeds past the disabled check to the total risk
// filter and, being critical and not previously notified, produces one event.
func TestProduceEntriesToKafkaNotifiesRuleNotDisabled(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "cluster-1",
		UpdatedAt:     types.Timestamp(testTimestamp),
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
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(1), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Target:   types.NotificationBackendTarget,
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	reportItems := types.ReportContent{buildReportItem(disabledRuleModule, disabledRuleErrorKey)}
	sent, err := differ.ProduceEntriesToKafka(&d, cluster, buildCriticalRuleContent(), reportItems, "{}")

	assert.NoError(t, err)
	assert.Equal(t, 1, sent, "a rule that is not disabled and is critical must produce one event")

	executionLog := buf.String()
	assert.NotContains(t, executionLog, differ.DisabledRuleSkippedMessage, "a non-disabled rule must not be skipped as disabled")
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage, "a non-disabled critical rule should reach the notification stage")

	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
}

// TestProduceEntriesToKafkaNonDisabledLowRiskReachesRiskFilter verifies that a
// rule that is not disabled but below the total risk threshold is dropped by the
// total risk filter (not by the disabled check), proving a non-disabled rule
// proceeds to the total risk filter.
func TestProduceEntriesToKafkaNonDisabledLowRiskReachesRiskFilter(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "cluster-1",
		UpdatedAt:     types.Timestamp(testTimestamp),
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

	d := differ.Differ{
		Storage:  &storage,
		Target:   types.NotificationBackendTarget,
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	// impact 1, likelihood 1 => totalRisk 1, below the default threshold of 2.
	lowRiskContent := buildRuleContentWith(1, 1)
	reportItems := types.ReportContent{buildReportItem(disabledRuleModule, disabledRuleErrorKey)}
	sent, err := differ.ProduceEntriesToKafka(&d, cluster, lowRiskContent, reportItems, "{}")

	assert.NoError(t, err)
	assert.Equal(t, 0, sent, "a rule below the total risk threshold must not produce an event")

	executionLog := buf.String()
	assert.NotContains(t, executionLog, differ.DisabledRuleSkippedMessage, "a non-disabled rule must not be skipped by the disabled check")

	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProduceEntriesToKafkaSkipsOnlyDisabledRuleInMixedReport verifies per-rule
// granularity: in a report with one disabled and one enabled critical rule, only
// the enabled rule is notified (see BDD "only the re-enabled rule is notified").
func TestProduceEntriesToKafkaSkipsOnlyDisabledRuleInMixedReport(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	const (
		enabledModule   = "ccx_rules_ocp.external.rules.other_rule.report"
		enabledRuleName = "other_rule"
		enabledErrorKey = "OTHER_RULE_CRITICAL_IMPACT"
	)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "cluster-1",
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	ruleContent := buildCriticalRuleContent()
	ruleContent[enabledRuleName] = utypes.RuleContent{
		ErrorKeys: map[string]utypes.RuleErrorKeyContent{
			enabledErrorKey: {
				Metadata: utypes.ErrorKeyMetadata{
					Description: "other rule error key description",
					Impact: utypes.Impact{
						Name:   "impact",
						Impact: 4,
					},
					Likelihood: 4,
				},
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
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(int32(1), int64(1), nil)

	d := differ.Differ{
		Storage:  &storage,
		Target:   types.NotificationBackendTarget,
		Filter:   differ.DefaultEventFilter,
		Notifier: &producerMock,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "cluster-1", RuleID: disabledRuleShortName, ErrorKey: disabledRuleErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	reportItems := types.ReportContent{
		buildReportItem(disabledRuleModule, disabledRuleErrorKey),
		buildReportItem(enabledModule, enabledErrorKey),
	}
	sent, err := differ.ProduceEntriesToKafka(&d, cluster, ruleContent, reportItems, "{}")

	assert.NoError(t, err)
	assert.Equal(t, 1, sent, "only the enabled rule should be notified")

	executionLog := buf.String()
	assert.Contains(t, executionLog, differ.DisabledRuleSkippedMessage, "the disabled rule should be logged as skipped")

	// exactly one message with a single event is produced for the enabled rule.
	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
}
