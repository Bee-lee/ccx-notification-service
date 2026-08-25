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
	"encoding/json"
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

// These tests cover the disabled rules filtering added to the Kafka processing
// path (CCXDEV-16567). The report JSON encodes a rule as a composite
// rule_id|error_key value in reports[].rule_id and a fully qualified module in
// reports[].component, while the aggregator tables store the plain rule ID and
// error key as separate columns. The rule name is therefore derived from the
// module via moduleToRuleName so it matches the aggregator's rule_id.

const (
	// module resolves to the rule name "test_rule" via moduleToRuleName.
	disabledTestModule = types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	disabledTestRuleID = types.RuleID("test_rule")

	criticalErrorKey  = types.ErrorKey("TEST_RULE_CRITICAL_IMPACT")
	importantErrorKey = types.ErrorKey("TEST_RULE_IMPORTANT_IMPACT")

	disabledTestCluster = types.ClusterName("5d5892d4-2g85-4ccf-02bg-548dfc9767aa")
)

// disabledRulesRuleContent returns rule content for "test_rule" with a critical
// (total risk 4) and an important (total risk 3) error key.
func disabledRulesRuleContent() types.RulesMap {
	return types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys: map[string]utypes.RuleErrorKeyContent{
				string(criticalErrorKey): {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "critical impact",
						Impact:      utypes.Impact{Name: "critical", Impact: 4},
						Likelihood:  4,
					},
				},
				string(importantErrorKey): {
					Metadata: utypes.ErrorKeyMetadata{
						Description: "important impact",
						Impact:      utypes.Impact{Name: "important", Impact: 3},
						Likelihood:  3,
					},
				},
			},
		},
	}
}

// makeReportItem builds a single evaluated report item for the given error key
// using the shared test module.
func makeReportItem(errorKey types.ErrorKey) *types.EvaluatedReportItem {
	return &types.EvaluatedReportItem{
		ReportItem: types.ReportItem{
			Type:     "rule",
			Module:   disabledTestModule,
			ErrorKey: errorKey,
		},
	}
}

// disabledRulesCluster returns a cluster entry for the given org ID.
func disabledRulesCluster(orgID types.OrgID) types.ClusterEntry {
	return types.ClusterEntry{
		OrgID:         orgID,
		AccountNumber: 1,
		ClusterName:   disabledTestCluster,
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}
}

// ---------------------------------------------------------------------------
// isRuleDisabled unit tests
// ---------------------------------------------------------------------------

// TestIsRuleDisabled verifies that isRuleDisabled matches a rule against both
// the per-cluster (cluster_rule_toggle) and org-wide (rule_disable) maps and is
// correctly scoped by cluster ID, org ID, rule ID and error key.
func TestIsRuleDisabled(t *testing.T) {
	const otherCluster = types.ClusterName("11111111-2222-3333-4444-555555555555")

	clusterKey := types.ClusterRuleKey{
		ClusterID: disabledTestCluster,
		RuleID:    disabledTestRuleID,
		ErrorKey:  criticalErrorKey,
	}
	orgKey := types.OrgRuleKey{
		OrgID:    "1",
		RuleID:   disabledTestRuleID,
		ErrorKey: criticalErrorKey,
	}

	testCases := []struct {
		name             string
		clusterDisabled  types.ClusterDisabledRules
		orgDisabled      types.OrgDisabledRules
		cluster          types.ClusterEntry
		errorKey         types.ErrorKey
		expectedDisabled bool
	}{
		{
			// AC: rule present in cluster-level map is skipped
			name:             "cluster-level disabled rule is reported as disabled",
			clusterDisabled:  types.ClusterDisabledRules{clusterKey: {}},
			cluster:          disabledRulesCluster(1),
			errorKey:         criticalErrorKey,
			expectedDisabled: true,
		},
		{
			// AC: rule present in org-level map is skipped. Also exercises the
			// numeric OrgID -> string conversion (org 1 -> "1").
			name:             "org-level disabled rule is reported as disabled",
			orgDisabled:      types.OrgDisabledRules{orgKey: {}},
			cluster:          disabledRulesCluster(1),
			errorKey:         criticalErrorKey,
			expectedDisabled: true,
		},
		{
			name:             "rule present in both maps is reported as disabled",
			clusterDisabled:  types.ClusterDisabledRules{clusterKey: {}},
			orgDisabled:      types.OrgDisabledRules{orgKey: {}},
			cluster:          disabledRulesCluster(1),
			errorKey:         criticalErrorKey,
			expectedDisabled: true,
		},
		{
			// AC: rule not in either map proceeds (not disabled)
			name:             "rule absent from both maps is not disabled",
			clusterDisabled:  types.ClusterDisabledRules{},
			orgDisabled:      types.OrgDisabledRules{},
			cluster:          disabledRulesCluster(1),
			errorKey:         criticalErrorKey,
			expectedDisabled: false,
		},
		{
			// cluster-level disable is scoped to a specific cluster
			name:             "cluster-level disable does not apply to a different cluster",
			clusterDisabled:  types.ClusterDisabledRules{clusterKey: {}},
			cluster:          types.ClusterEntry{OrgID: 1, ClusterName: otherCluster},
			errorKey:         criticalErrorKey,
			expectedDisabled: false,
		},
		{
			// org-wide ack for org 1 must not affect a cluster in org 2
			// (BDD: rule ack does not affect other organizations)
			name:             "org-level disable does not apply to a different org",
			orgDisabled:      types.OrgDisabledRules{orgKey: {}},
			cluster:          disabledRulesCluster(2),
			errorKey:         criticalErrorKey,
			expectedDisabled: false,
		},
		{
			// matching happens on the error key too, not only the rule ID
			name:             "same rule but different error key is not disabled",
			clusterDisabled:  types.ClusterDisabledRules{clusterKey: {}},
			orgDisabled:      types.OrgDisabledRules{orgKey: {}},
			cluster:          disabledRulesCluster(1),
			errorKey:         importantErrorKey,
			expectedDisabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := differ.Differ{
				ClusterDisabledRules: tc.clusterDisabled,
				OrgDisabledRules:     tc.orgDisabled,
			}
			disabled := differ.IsRuleDisabled(&d, tc.cluster, types.RuleName(disabledTestRuleID), tc.errorKey)
			assert.Equal(t, tc.expectedDisabled, disabled)
		})
	}
}

// ---------------------------------------------------------------------------
// produceEntriesToKafka integration tests (disabled rules filtering)
// ---------------------------------------------------------------------------

// newProducerCapturingMessages returns a Producer mock that records every
// message payload it is asked to produce and always reports success (offset 0).
func newProducerCapturingMessages(captured *[][]byte) *mocks.Producer {
	producerMock := &mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		func(msg types.ProducerMessage) int32 { return 0 },
		func(msg types.ProducerMessage) int64 {
			*captured = append(*captured, msg)
			return 0
		},
		func(msg types.ProducerMessage) error { return nil },
	)
	return producerMock
}

// newRecordingStorage returns a Storage mock that accepts any notification
// record write and reports success.
func newRecordingStorage() *mocks.Storage {
	storage := &mocks.Storage{}
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
	return storage
}

func newDifferForKafka(storage differ.Storage, notifier *mocks.Producer, clusterDisabled types.ClusterDisabledRules, orgDisabled types.OrgDisabledRules) differ.Differ {
	return differ.Differ{
		Storage:          storage,
		Notifier:         notifier,
		NotificationType: types.InstantNotif,
		Target:           types.NotificationBackendTarget,
		Thresholds: differ.EventThresholds{
			TotalRisk: differ.DefaultTotalRiskThreshold,
		},
		Filter:               differ.DefaultEventFilter,
		ClusterDisabledRules: clusterDisabled,
		OrgDisabledRules:     orgDisabled,
	}
}

// TestProduceEntriesToKafkaSkipsClusterDisabledRule verifies that a rule listed
// in the cluster_rule_toggle map is skipped entirely: no notification message
// is produced and the "high impact" evaluation (which precedes ShouldNotify) is
// never reached.
func TestProduceEntriesToKafkaSkipsClusterDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := disabledRulesCluster(1)
	clusterDisabled := types.ClusterDisabledRules{
		{ClusterID: cluster.ClusterName, RuleID: disabledTestRuleID, ErrorKey: criticalErrorKey}: {},
	}

	storage := newRecordingStorage()
	var captured [][]byte
	notifier := newProducerCapturingMessages(&captured)

	d := newDifferForKafka(storage, notifier, clusterDisabled, types.OrgDisabledRules{})

	reportItems := types.ReportContent{makeReportItem(criticalErrorKey)}
	messages, err := differ.ProduceEntriesToKafka(&d, cluster, disabledRulesRuleContent(), reportItems, "")

	assert.NoError(t, err)
	assert.Equal(t, 0, messages, "disabled rule must not produce any notification event")
	notifier.AssertNotCalled(t, "ProduceMessage", mock.Anything)
	assert.NotContains(t, buf.String(), differ.ReportWithHighImpactMessage,
		"a disabled rule must never reach the total risk filter or ShouldNotify")
}

// TestProduceEntriesToKafkaSkipsOrgDisabledRule verifies that a rule listed in
// the rule_disable (org ack) map is skipped entirely for a cluster in that org.
func TestProduceEntriesToKafkaSkipsOrgDisabledRule(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := disabledRulesCluster(1)
	orgDisabled := types.OrgDisabledRules{
		{OrgID: "1", RuleID: disabledTestRuleID, ErrorKey: criticalErrorKey}: {},
	}

	storage := newRecordingStorage()
	var captured [][]byte
	notifier := newProducerCapturingMessages(&captured)

	d := newDifferForKafka(storage, notifier, types.ClusterDisabledRules{}, orgDisabled)

	reportItems := types.ReportContent{makeReportItem(criticalErrorKey)}
	messages, err := differ.ProduceEntriesToKafka(&d, cluster, disabledRulesRuleContent(), reportItems, "")

	assert.NoError(t, err)
	assert.Equal(t, 0, messages, "org-acked rule must not produce any notification event")
	notifier.AssertNotCalled(t, "ProduceMessage", mock.Anything)
	assert.NotContains(t, buf.String(), differ.ReportWithHighImpactMessage,
		"a disabled rule must never reach the total risk filter or ShouldNotify")
}

// TestProduceEntriesToKafkaOrgDisableDoesNotAffectOtherOrg verifies that an
// org-wide ack for org 1 does not suppress the rule for a cluster in org 2.
func TestProduceEntriesToKafkaOrgDisableDoesNotAffectOtherOrg(t *testing.T) {
	cluster := disabledRulesCluster(2)
	orgDisabled := types.OrgDisabledRules{
		{OrgID: "1", RuleID: disabledTestRuleID, ErrorKey: criticalErrorKey}: {},
	}

	storage := newRecordingStorage()
	var captured [][]byte
	notifier := newProducerCapturingMessages(&captured)

	d := newDifferForKafka(storage, notifier, types.ClusterDisabledRules{}, orgDisabled)

	reportItems := types.ReportContent{makeReportItem(criticalErrorKey)}
	messages, err := differ.ProduceEntriesToKafka(&d, cluster, disabledRulesRuleContent(), reportItems, "")

	assert.NoError(t, err)
	assert.Equal(t, 1, messages, "rule acked for a different org must still be notified")
	notifier.AssertNumberOfCalls(t, "ProduceMessage", 1)
}

// TestProduceEntriesToKafkaNotDisabledRuleProceeds verifies that when no
// disabled rules apply, the rule passes the total risk filter and ShouldNotify,
// producing a notification event.
func TestProduceEntriesToKafkaNotDisabledRuleProceeds(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	cluster := disabledRulesCluster(1)

	storage := newRecordingStorage()
	var captured [][]byte
	notifier := newProducerCapturingMessages(&captured)

	d := newDifferForKafka(storage, notifier, types.ClusterDisabledRules{}, types.OrgDisabledRules{})

	reportItems := types.ReportContent{makeReportItem(criticalErrorKey)}
	messages, err := differ.ProduceEntriesToKafka(&d, cluster, disabledRulesRuleContent(), reportItems, "")

	assert.NoError(t, err)
	assert.Equal(t, 1, messages, "a non-disabled rule above the threshold must produce one event")
	notifier.AssertNumberOfCalls(t, "ProduceMessage", 1)
	assert.Contains(t, buf.String(), differ.ReportWithHighImpactMessage,
		"a non-disabled rule must reach the total risk filter and ShouldNotify")
}

// TestProduceEntriesToKafkaSkipsOnlyDisabledRuleAmongMany verifies that when a
// report contains several rules and only one is disabled, the disabled rule is
// skipped while the remaining rule is still notified. This mirrors the BDD
// scenario where only the non-disabled rule is included in the notification.
func TestProduceEntriesToKafkaSkipsOnlyDisabledRuleAmongMany(t *testing.T) {
	cluster := disabledRulesCluster(1)
	// disable only the critical rule for this cluster
	clusterDisabled := types.ClusterDisabledRules{
		{ClusterID: cluster.ClusterName, RuleID: disabledTestRuleID, ErrorKey: criticalErrorKey}: {},
	}

	storage := newRecordingStorage()
	var captured [][]byte
	notifier := newProducerCapturingMessages(&captured)

	d := newDifferForKafka(storage, notifier, clusterDisabled, types.OrgDisabledRules{})

	reportItems := types.ReportContent{
		makeReportItem(criticalErrorKey),
		makeReportItem(importantErrorKey),
	}
	messages, err := differ.ProduceEntriesToKafka(&d, cluster, disabledRulesRuleContent(), reportItems, "")

	assert.NoError(t, err)
	assert.Equal(t, 1, messages, "only the non-disabled rule should be notified")
	notifier.AssertNumberOfCalls(t, "ProduceMessage", 1)

	// confirm the surviving event is the important (non-disabled) rule, not the
	// disabled critical one
	assert.Len(t, captured, 1)
	var msg types.NotificationMessage
	err = json.Unmarshal(captured[0], &msg)
	assert.NoError(t, err)
	assert.Len(t, msg.Events, 1)
	assert.Equal(t, "3", msg.Events[0].Payload["total_risk"],
		"the surviving event must be the important (non-disabled) rule")
}
