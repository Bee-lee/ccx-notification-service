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
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
	utypes "github.com/RedHatInsights/insights-results-types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- isRuleDisabled unit tests ---

// TestIsRuleDisabledClusterLevel verifies that a rule found in the
// cluster-level disabled rules map (cluster_rule_toggle) is reported
// as disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-1f74-4ccf-91af-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.True(t, result, "rule in cluster-level disabled map should be reported as disabled")
}

// TestIsRuleDisabledOrgLevel verifies that a rule found in the
// org-level disabled rules map (rule_disable) is reported as disabled.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.True(t, result, "rule in org-level disabled map should be reported as disabled")
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule not found in
// either disabled rules map is reported as not disabled (proceeds to
// the total risk filter).
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.False(t, result, "rule not in any disabled map should not be reported as disabled")
}

// TestIsRuleDisabledDifferentCluster verifies that a cluster-level
// disabled rule for one cluster does not affect a different cluster.
func TestIsRuleDisabledDifferentCluster(t *testing.T) {
	otherCluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-1f74-4ccf-91af-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, otherCluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.False(t, result, "cluster-level disable for a different cluster should not match")
}

// TestIsRuleDisabledDifferentOrg verifies that an org-level disabled
// rule for one organization does not affect a different organization
// (BDD: "rule ack for an organization does not affect other organizations").
func TestIsRuleDisabledDifferentOrg(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       2,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.False(t, result, "org-level disable for org 1 should not affect org 2")
}

// TestIsRuleDisabledDifferentErrorKey verifies that a disabled rule
// with a different error key does not match.
func TestIsRuleDisabledDifferentErrorKey(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-1f74-4ccf-91af-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_IMPORTANT_IMPACT"}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.False(t, result, "disabled rule with different error key should not match")
}

// TestIsRuleDisabledBothMapsContainRule verifies that when a rule is
// in both cluster-level and org-level maps, it is still reported as
// disabled (no double-counting or error).
func TestIsRuleDisabledBothMapsContainRule(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{ClusterID: "5d5892d4-1f74-4ccf-91af-548dfc9767aa", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "1", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.True(t, result, "rule in both maps should be reported as disabled")
}

// TestIsRuleDisabledEmptyMaps verifies that with nil/empty maps a rule
// is not disabled and no panic occurs.
func TestIsRuleDisabledEmptyMaps(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	assert.NotPanics(t, func() {
		result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
		assert.False(t, result)
	})
}

// TestIsRuleDisabledOrgIDConversion verifies that the OrgID (uint32)
// is correctly converted to string for the OrgRuleKey lookup. OrgID 10
// should match OrgRuleKey.OrgID "10".
func TestIsRuleDisabledOrgIDConversion(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       10,
		ClusterName: "5d5892d4-1f74-4ccf-91af-548dfc9767aa",
	}

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{OrgID: "10", RuleID: "test_rule", ErrorKey: "TEST_RULE_CRITICAL_IMPACT"}: {},
		},
	}

	result := differ.IsRuleDisabled(&d, cluster, "test_rule", "TEST_RULE_CRITICAL_IMPACT")
	assert.True(t, result, "OrgID 10 should match OrgRuleKey with OrgID '10'")
}

// --- Integration-level tests for produceEntriesToKafka disabled rules filtering ---

// setupMockBroker creates a mock Sarama broker for Kafka tests.
func setupMockBroker(t *testing.T) *sarama.MockBroker {
	mockBroker := sarama.NewMockBroker(t, 0)
	mockBroker.SetHandlerByMap(
		map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(mockBroker.Addr(), mockBroker.BrokerID()).
				SetLeader(brokerCfg.Topic, 0, mockBroker.BrokerID()),
			"OffsetRequest": sarama.NewMockOffsetResponse(t).
				SetOffset(brokerCfg.Topic, 0, -1, 0).
				SetOffset(brokerCfg.Topic, 0, -2, 0),
			"FetchRequest": sarama.NewMockFetchResponse(t, 1),
			"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
				SetCoordinator(sarama.CoordinatorGroup, "", mockBroker),
			"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
				SetOffset("", brokerCfg.Topic, 0, 0, "", sarama.ErrNoError),
		})
	return mockBroker
}

// defaultRuleContent returns a minimal RulesMap with one rule that
// has totalRisk >= 3 (critical), matching the threshold to produce
// a notification.
func defaultRuleContent() types.RulesMap {
	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact description",
				Impact: utypes.Impact{
					Name:   "impact_critical",
					Impact: 4,
				},
				Likelihood: 4,
			},
			HasReason: true,
		},
	}
	return types.RulesMap{
		"test_rule": {
			Summary:    "test rule summary",
			Reason:     "test rule reason",
			Resolution: "test rule resolution",
			MoreInfo:   "test rule more info",
			ErrorKeys:  errorKeys,
			HasReason:  true,
		},
	}
}

// reportJSONWithRule returns a report JSON containing one rule hit
// using the composite "rule_id|error_key" format. The module follows
// the fully qualified naming convention.
func reportJSONWithRule() types.ClusterReport {
	return `{"analysis_metadata":{"metadata":"some metadata"},"reports":[{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"some details"}]}`
}

// TestProcessClustersKafkaDisabledRuleClusterLevel verifies that
// produceEntriesToKafka skips a rule that is disabled at the cluster
// level and does not produce any Kafka messages.
func TestProcessClustersKafkaDisabledRuleClusterLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}
	// No ProduceMessage call expected because the rule is disabled.

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		reportJSONWithRule(), nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		defaultRuleContent(), []types.ClusterEntry{cluster})

	executionLog := buf.String()
	assert.Contains(t, executionLog, "Rule is disabled, skipping",
		"disabled rule should produce a skip log message")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced for a disabled rule")
	// ProduceMessage should never have been called
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersKafkaDisabledRuleOrgLevel verifies that
// produceEntriesToKafka skips a rule that is disabled at the org
// level (rule_disable / rule ack) and does not produce any Kafka messages.
func TestProcessClustersKafkaDisabledRuleOrgLevel(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		reportJSONWithRule(), nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		defaultRuleContent(), []types.ClusterEntry{cluster})

	executionLog := buf.String()
	assert.Contains(t, executionLog, "Rule is disabled, skipping",
		"disabled rule should produce a skip log message")
	assert.NotContains(t, executionLog, "Producing instant notification",
		"no notification should be produced for an org-level disabled rule")
	producerMock.AssertNotCalled(t, "ProduceMessage", mock.Anything)
}

// TestProcessClustersKafkaNotDisabledRuleProceeds verifies that when a
// rule is not in either disabled map, it proceeds through the total
// risk filter and reaches ShouldNotify and produces a Kafka message.
func TestProcessClustersKafkaNotDisabledRuleProceeds(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		reportJSONWithRule(), nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		defaultRuleContent(), []types.ClusterEntry{cluster})

	executionLog := buf.String()
	assert.NotContains(t, executionLog, "Rule is disabled, skipping",
		"non-disabled rule should not produce a skip log message")
	assert.Contains(t, executionLog, "Producing instant notification",
		"non-disabled rule should proceed to produce a notification")
	producerMock.AssertCalled(t, "ProduceMessage", mock.AnythingOfType("types.ProducerMessage"))
}

// TestProcessClustersKafkaDisabledRuleOrgDoesNotAffectOtherOrg verifies
// that an org-level rule ack for org 1 does not suppress the rule for
// org 2 (BDD: "rule ack for an organization does not affect other
// organizations").
func TestProcessClustersKafkaDisabledRuleOrgDoesNotAffectOtherOrg(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	clusterOrg1 := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}
	clusterOrg2 := types.ClusterEntry{
		OrgID:         2,
		AccountNumber: 2,
		ClusterName:   "7e6903e5-3h96-5ddf-13ch-659efd8878bb",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		reportJSONWithRule(), nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		defaultRuleContent(), []types.ClusterEntry{clusterOrg1, clusterOrg2})

	executionLog := buf.String()
	// org 2 cluster should still produce a notification
	assert.Contains(t, executionLog,
		fmt.Sprintf(`"cluster":"%s"`, clusterOrg2.ClusterName),
		"notification should be produced for org 2 cluster")
	assert.Contains(t, executionLog, "Producing instant notification",
		"org 2 should get a notification even though org 1 has the rule disabled")
	producerMock.AssertCalled(t, "ProduceMessage", mock.AnythingOfType("types.ProducerMessage"))
}

// TestProcessClustersKafkaDisabledRuleNeverReachesShouldNotify verifies
// that a disabled rule is skipped before ShouldNotify is reached. This
// is checked by not setting up ShouldNotify preconditions (no
// ReadLastNNotifiedRecords mock needed for the disabled path) and
// verifying no "report with high impact" log is emitted.
func TestProcessClustersKafkaDisabledRuleNeverReachesShouldNotify(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(
		reportJSONWithRule(), nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		defaultRuleContent(), []types.ClusterEntry{cluster})

	executionLog := buf.String()
	assert.NotContains(t, executionLog, differ.ReportWithHighImpactMessage,
		"disabled rule should never reach the high-impact reporting path")
}

// TestProcessClustersKafkaOnlyDisabledRuleSkippedOthersPass verifies
// that when a report contains multiple rules, only the disabled one
// is skipped — the other rule proceeds through to produce a notification.
// (BDD: "only the disabled rule is skipped, other rules are notified")
func TestProcessClustersKafkaOnlyDisabledRuleSkippedOthersPass(t *testing.T) {
	buf := new(bytes.Buffer)
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(zerolog.WarnLevel)

	mockBroker := setupMockBroker(t)
	defer mockBroker.Close()

	producerMock := mocks.Producer{}
	producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).Return(
		int32(0), int64(0), nil)

	cluster := types.ClusterEntry{
		OrgID:         1,
		AccountNumber: 1,
		ClusterName:   "5d5892d4-2g85-4ccf-02bg-548dfc9767aa",
		KafkaOffset:   0,
		UpdatedAt:     types.Timestamp(testTimestamp),
	}

	// Report with two rules: test_rule (critical) and test_rule (important)
	reportJSON := types.ClusterReport(`{"analysis_metadata":{"metadata":"some metadata"},"reports":[` +
		`{"rule_id":"test_rule|TEST_RULE_CRITICAL_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_CRITICAL_IMPACT","details":"some details"},` +
		`{"rule_id":"test_rule|TEST_RULE_IMPORTANT_IMPACT","component":"ccx_rules_ocp.external.rules.test_rule.report","type":"rule","key":"TEST_RULE_IMPORTANT_IMPACT","details":"some details"}` +
		`]}`)

	errorKeys := map[string]utypes.RuleErrorKeyContent{
		"TEST_RULE_CRITICAL_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule critical impact",
				Impact:      utypes.Impact{Name: "critical", Impact: 4},
				Likelihood:  4,
			},
			HasReason: true,
		},
		"TEST_RULE_IMPORTANT_IMPACT": {
			Metadata: utypes.ErrorKeyMetadata{
				Description: "test rule important impact",
				Impact:      utypes.Impact{Name: "important", Impact: 3},
				Likelihood:  3,
			},
			HasReason: true,
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

	storage := mocks.Storage{}
	storage.On("ReadReportForClusterAtTime",
		mock.AnythingOfType("types.OrgID"),
		mock.AnythingOfType("types.ClusterName"),
		mock.AnythingOfType("types.Timestamp")).Return(reportJSON, nil)
	storage.On("ReadLastNNotifiedRecords",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("int")).Return([]types.NotificationRecord{}, nil)
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).Return(nil)

	// Only disable the critical rule — the important rule should still go through
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

	d.ProcessClusters(&conf.ConfigStruct{Kafka: conf.KafkaConfiguration{Enabled: true}},
		ruleContent, []types.ClusterEntry{cluster})

	executionLog := buf.String()
	// The disabled rule should be skipped
	assert.Contains(t, executionLog, "Rule is disabled, skipping")
	// The important rule should produce a notification (totalRisk = (3+3)/2 = 3 >= 3)
	assert.Contains(t, executionLog, "Producing instant notification",
		"non-disabled rule should produce a notification")
	producerMock.AssertCalled(t, "ProduceMessage", mock.AnythingOfType("types.ProducerMessage"))
}
