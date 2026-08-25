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

// Unit tests for filtering disabled rules in the Kafka processing path
// (CCXDEV-16567). They cover the composite "rule_id|error_key" parsing helper,
// the per-rule disabled lookup against the cluster-level and org-level maps, and
// the behaviour of produceEntriesToKafka when a rule is disabled versus enabled.

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
	testRuleID         = types.RuleID("test_rule")
	testErrorKey       = types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")
	testModule         = types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	testCompositeRule  = types.RuleID("test_rule|TEST_RULE_CRITICAL_IMPACT")
	importantErrorKey  = types.ErrorKey("TEST_RULE_IMPORTANT_IMPACT")
	importantComposite = types.RuleID("test_rule|TEST_RULE_IMPORTANT_IMPACT")
)

// disabledRulesTestCluster mirrors the org/cluster identifiers used in the
// notifications_disabled_rules.feature BDD scenarios.
var disabledRulesTestCluster = types.ClusterEntry{
	OrgID:         1,
	AccountNumber: 1,
	ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
	KafkaOffset:   0,
	UpdatedAt:     types.Timestamp(testTimestamp),
}

// disabledRulesRuleContent provides rule content so that findRuleByNameAndErrorKey
// resolves a total risk above the default threshold for both error keys.
func disabledRulesRuleContent() types.RulesMap {
	return types.RulesMap{
		"test_rule": {
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				string(testErrorKey): {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact error key",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
				},
				string(importantErrorKey): {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "important impact error key",
						Impact:      utypes.Impact{Name: "important", Impact: 3},
						Likelihood:  3,
					},
				},
			},
		},
	}
}

// newReportItem builds a single evaluated report item using the composite
// rule_id format stored in the report JSON.
func newReportItem(compositeRuleID types.RuleID, module types.ModuleName, errorKey types.ErrorKey) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			RuleID:   compositeRuleID,
			Module:   module,
			ErrorKey: errorKey,
			Details:  []byte("details of the issue"),
		},
	}
}

// --- parseCompositeRuleID tests ---

// TestParseCompositeRuleID verifies that the composite "rule_id|error_key"
// identifier used in the report JSON is split into its separate parts.
func TestParseCompositeRuleID(t *testing.T) {
	type testCase struct {
		name             string
		composite        types.RuleID
		expectedRuleID   types.RuleID
		expectedErrorKey types.ErrorKey
	}

	testCases := []testCase{
		{
			name:             "standard composite",
			composite:        testCompositeRule,
			expectedRuleID:   testRuleID,
			expectedErrorKey: testErrorKey,
		},
		{
			name:             "no separator treats whole value as rule_id",
			composite:        "test_rule",
			expectedRuleID:   "test_rule",
			expectedErrorKey: "",
		},
		{
			name:             "empty value",
			composite:        "",
			expectedRuleID:   "",
			expectedErrorKey: "",
		},
		{
			name:             "trailing separator yields empty error key",
			composite:        "test_rule|",
			expectedRuleID:   "test_rule",
			expectedErrorKey: "",
		},
		{
			name:             "only first separator is used",
			composite:        "test_rule|ERROR|EXTRA",
			expectedRuleID:   "test_rule",
			expectedErrorKey: "ERROR|EXTRA",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ruleID, errorKey := differ.ParseCompositeRuleID(tc.composite)
			assert.Equal(t, tc.expectedRuleID, ruleID)
			assert.Equal(t, tc.expectedErrorKey, errorKey)
		})
	}
}

// --- isRuleDisabled tests ---

// TestIsRuleDisabledClusterLevel verifies that a rule present in the
// cluster-level map (cluster_rule_toggle) is reported as disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: disabledRulesTestCluster.ClusterName, RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.True(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, testErrorKey))
}

// TestIsRuleDisabledOrgLevel verifies that a rule present in the org-level map
// (rule_disable) is reported as disabled. The org ID is compared as a string.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
	}

	assert.True(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, testErrorKey))
}

// TestIsRuleDisabledNotDisabled verifies that a rule absent from both maps is
// not reported as disabled.
func TestIsRuleDisabledNotDisabled(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, testErrorKey))
}

// TestIsRuleDisabledClusterScopedToCluster verifies that a cluster-level disable
// for one cluster does not affect a different cluster.
func TestIsRuleDisabledClusterScopedToCluster(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "some-other-cluster", RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, testErrorKey))
}

// TestIsRuleDisabledOrgScopedToOrg verifies that an org-level ack for one
// organization does not affect a cluster belonging to another organization.
func TestIsRuleDisabledOrgScopedToOrg(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "2", RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, testErrorKey))
}

// TestIsRuleDisabledErrorKeyMustMatch verifies that a matching rule_id with a
// different error_key is not reported as disabled.
func TestIsRuleDisabledErrorKeyMustMatch(t *testing.T) {
	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: disabledRulesTestCluster.ClusterName, RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, disabledRulesTestCluster, testRuleID, importantErrorKey))
}

// --- produceEntriesToKafka tests ---

// newDisabledRulesProducerMock returns a Producer mock whose ProduceMessage
// always succeeds (returns a valid offset).
func newDisabledRulesProducerMock() *mocks.Producer {
	producerMock := &mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0),
		int64(1),
		nil,
	)
	return producerMock
}

// newDisabledRulesStorageMock returns a Storage mock accepting the
// WriteNotificationRecordForCluster call made when the outcome is recorded.
func newDisabledRulesStorageMock() *mocks.Storage {
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

// newDisabledRulesDiffer wires a Differ ready to run produceEntriesToKafka with
// the provided disabled-rule maps. PreviouslyReported is left empty so that any
// rule reaching ShouldNotify is treated as new and would be notified.
func newDisabledRulesDiffer(
	producerMock *mocks.Producer,
	storage *mocks.Storage,
	clusterDisabled types.ClusterDisabledRules,
	orgDisabled types.OrgDisabledRules,
) differ.Differ {
	return differ.Differ{
		Storage:              storage,
		Notifier:             producerMock,
		Target:               types.NotificationBackendTarget,
		NotificationType:     types.InstantNotif,
		Thresholds:           differ.EventThresholds{TotalRisk: differ.DefaultTotalRiskThreshold},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: clusterDisabled,
		OrgDisabledRules:     orgDisabled,
	}
}

// TestProduceEntriesToKafkaClusterDisabledRuleSkipped verifies that a rule
// present in the cluster-level map is skipped entirely: no notification event is
// produced and ShouldNotify / the notification producer are never reached.
func TestProduceEntriesToKafkaClusterDisabledRuleSkipped(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	producerMock := newDisabledRulesProducerMock()
	storage := newDisabledRulesStorageMock()
	d := newDisabledRulesDiffer(
		producerMock,
		storage,
		types.ClusterDisabledRules{
			{ClusterID: disabledRulesTestCluster.ClusterName, RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
		make(types.OrgDisabledRules),
	)

	reportItems := types.ReportContent{
		newReportItem(testCompositeRule, testModule, testErrorKey),
	}

	notified, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesTestCluster, disabledRulesRuleContent(), reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, notified, "disabled rule must not produce a notification event")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)

	executionLog := buf.String()
	assert.Contains(t, executionLog, "Skipping disabled rule")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"disabled rule must not reach the total risk filter or ShouldNotify")
}

// TestProduceEntriesToKafkaOrgDisabledRuleSkipped verifies that a rule present
// in the org-level (rule_disable) map is skipped entirely.
func TestProduceEntriesToKafkaOrgDisabledRuleSkipped(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	producerMock := newDisabledRulesProducerMock()
	storage := newDisabledRulesStorageMock()
	d := newDisabledRulesDiffer(
		producerMock,
		storage,
		make(types.ClusterDisabledRules),
		types.OrgDisabledRules{
			{OrgID: "1", RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
	)

	reportItems := types.ReportContent{
		newReportItem(testCompositeRule, testModule, testErrorKey),
	}

	notified, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesTestCluster, disabledRulesRuleContent(), reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 0, notified, "org-disabled rule must not produce a notification event")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)

	executionLog := buf.String()
	assert.Contains(t, executionLog, "Skipping disabled rule")
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"org-disabled rule must not reach the total risk filter or ShouldNotify")
}

// TestProduceEntriesToKafkaEnabledRuleNotified verifies that a rule absent from
// both maps proceeds through the total risk filter and ShouldNotify and results
// in a notification event.
func TestProduceEntriesToKafkaEnabledRuleNotified(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	producerMock := newDisabledRulesProducerMock()
	storage := newDisabledRulesStorageMock()
	d := newDisabledRulesDiffer(
		producerMock,
		storage,
		make(types.ClusterDisabledRules),
		make(types.OrgDisabledRules),
	)

	reportItems := types.ReportContent{
		newReportItem(testCompositeRule, testModule, testErrorKey),
	}

	notified, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesTestCluster, disabledRulesRuleContent(), reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 1, notified, "enabled rule with high total risk must produce one notification event")
	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)

	executionLog := buf.String()
	assert.NotContains(t, executionLog, "Skipping disabled rule")
	assert.Contains(t, executionLog, differ.ReportWithHighImpactMessage,
		"enabled rule must reach the total risk filter")
}

// TestProduceEntriesToKafkaOnlyDisabledRuleSkipped verifies that when a report
// contains both a disabled and an enabled rule, only the enabled rule is
// notified. This mirrors the BDD scenario where one rule is disabled per cluster
// while another remains active.
func TestProduceEntriesToKafkaOnlyDisabledRuleSkipped(t *testing.T) {
	producerMock := newDisabledRulesProducerMock()
	storage := newDisabledRulesStorageMock()
	d := newDisabledRulesDiffer(
		producerMock,
		storage,
		types.ClusterDisabledRules{
			{ClusterID: disabledRulesTestCluster.ClusterName, RuleID: testRuleID, ErrorKey: testErrorKey}: {},
		},
		make(types.OrgDisabledRules),
	)

	reportItems := types.ReportContent{
		newReportItem(testCompositeRule, testModule, testErrorKey),
		newReportItem(importantComposite, testModule, importantErrorKey),
	}

	notified, err := differ.ProduceEntriesToKafka(
		&d, disabledRulesTestCluster, disabledRulesRuleContent(), reportItems, types.ClusterReport("{}"))

	assert.NoError(t, err)
	assert.Equal(t, 1, notified, "only the enabled rule should be notified")
	producerMock.AssertNumberOfCalls(t, "ProduceMessage", 1)
}
