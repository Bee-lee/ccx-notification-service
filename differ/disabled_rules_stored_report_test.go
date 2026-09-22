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

// Unit tests for omitting the rules disabled by the customer from the report
// written to the `reported` table (CCXDEV-16569). The stored report is the
// baseline IssueNotInReport compares against on the following runs, so leaving
// a disabled rule in it would make the rule look "already reported" once it is
// re-enabled and the customer would never be notified about it again.
//
// Covered here: filterDisabledRulesFromReport itself, the Kafka path (where the
// filtering happens in produceEntriesToKafka for all three record states) and
// the Service Log path (where it happens in processReportsByCluster, because
// ProduceEntriesToServiceLog neither receives nor returns the report).

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/tests/mocks"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

const (
	// criticalRuleCompositeID and importantRuleCompositeID are the composite
	// "rule_id|error_key" identifiers of the two rule hits of
	// storedReportTwoRulesJSON, in the very format the `report` column holds
	// and the design document uses when describing the `reported` table.
	criticalRuleCompositeID  = "test_rule|TEST_RULE_CRITICAL_IMPACT"
	importantRuleCompositeID = "test_rule|TEST_RULE_IMPORTANT_IMPACT"

	// storedReportTwoRulesJSON is a report as it is read from `new_reports` and,
	// until now, stored verbatim in the `reported` table. It carries two hits of
	// test_rule: the critical and the important one. Besides the fields this
	// service models (component, key, type, details) it also carries the ones it
	// does not (the composite rule_id, tags, links) plus the top level
	// analysis_metadata, none of which may be lost when disabled rules are
	// omitted.
	storedReportTwoRulesJSON = `{
		"analysis_metadata": {"metadata": "some metadata"},
		"reports": [
			{
				"rule_id": "test_rule|TEST_RULE_CRITICAL_IMPACT",
				"component": "ccx_rules_ocp.external.rules.test_rule.report",
				"type": "rule",
				"key": "TEST_RULE_CRITICAL_IMPACT",
				"details": {"detail": "critical details"},
				"tags": ["openshift"],
				"links": {"docs": ["https://example.com/critical"]}
			},
			{
				"rule_id": "test_rule|TEST_RULE_IMPORTANT_IMPACT",
				"component": "ccx_rules_ocp.external.rules.test_rule.report",
				"type": "rule",
				"key": "TEST_RULE_IMPORTANT_IMPACT",
				"details": {"detail": "important details"},
				"tags": ["openshift"],
				"links": {"docs": ["https://example.com/important"]}
			}
		]
	}`
)

// disabledRulesForCluster returns a cluster_rule_toggle map disabling the given
// error keys of test_rule for the given cluster.
func disabledRulesForCluster(cluster types.ClusterEntry, errorKeys ...types.ErrorKey) types.ClusterDisabledRules {
	disabled := make(types.ClusterDisabledRules, len(errorKeys))
	for _, errorKey := range errorKeys {
		disabled[types.ClusterRuleKey{
			ClusterID: cluster.ClusterName,
			RuleID:    disabledRulesTestRuleID,
			ErrorKey:  errorKey,
		}] = struct{}{}
	}
	return disabled
}

// disabledRulesForOrg returns a rule_disable map acking the given error keys of
// test_rule for the given organization.
func disabledRulesForOrg(orgID string, errorKeys ...types.ErrorKey) types.OrgDisabledRules {
	disabled := make(types.OrgDisabledRules, len(errorKeys))
	for _, errorKey := range errorKeys {
		disabled[types.OrgRuleKey{
			OrgID:    orgID,
			RuleID:   disabledRulesTestRuleID,
			ErrorKey: errorKey,
		}] = struct{}{}
	}
	return disabled
}

// storedReportRuleIDs returns the composite "rule_id" of every rule hit left in
// the given report. Reading the composite identifier (and not the component)
// also proves that the fields this service does not model survive the filtering
// of the hits around them.
func storedReportRuleIDs(t *testing.T, report types.ClusterReport) []string {
	t.Helper()

	var document struct {
		Reports []struct {
			RuleID string `json:"rule_id"`
		} `json:"reports"`
	}
	err := json.Unmarshal([]byte(report), &document)
	assert.NoError(t, err, "the stored report must remain a valid report document")

	ruleIDs := make([]string, 0, len(document.Reports))
	for _, reportItem := range document.Reports {
		ruleIDs = append(ruleIDs, reportItem.RuleID)
	}
	return ruleIDs
}

// storedReportHasReportsField tells whether the given report still has a
// "reports" field, which distinguishes an empty list of rule hits from a report
// that lost the field altogether.
func storedReportHasReportsField(t *testing.T, report types.ClusterReport) bool {
	t.Helper()

	var document map[string]json.RawMessage
	err := json.Unmarshal([]byte(report), &document)
	assert.NoError(t, err, "the stored report must remain a valid JSON document")

	_, found := document["reports"]
	return found
}

// newStoredReportStorageMock returns a Storage mock that records every report
// handed to WriteNotificationRecordForCluster, which is what ends up in the
// `report` column of the `reported` table.
func newStoredReportStorageMock(storedReports *[]types.ClusterReport) *mocks.Storage {
	storage := &mocks.Storage{}
	storage.On("WriteNotificationRecordForCluster",
		mock.AnythingOfType("types.ClusterEntry"),
		mock.AnythingOfType("types.NotificationTypeID"),
		mock.AnythingOfType("types.StateID"),
		mock.AnythingOfType("types.ClusterReport"),
		mock.AnythingOfType("types.Timestamp"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("types.EventTarget")).
		Run(func(args mock.Arguments) {
			*storedReports = append(*storedReports, args.Get(3).(types.ClusterReport))
		}).
		Return(nil)
	return storage
}

// storedReportTestReportItems deserializes the shared report fixture the same
// way processReportsByCluster does before handing the rule hits over to the
// target specific code.
func storedReportTestReportItems(t *testing.T) types.ReportContent {
	t.Helper()

	var deserialized types.Report
	err := json.Unmarshal([]byte(storedReportTwoRulesJSON), &deserialized)
	assert.NoError(t, err)
	assert.Len(t, deserialized.Reports, 2, "both rule hits must be deserialized from the fixture")
	return deserialized.Reports
}

// TestFilterDisabledRulesFromReport verifies that the report to be stored in the
// `reported` table keeps the active rules only. Both disabled-rule sources are
// exercised: the per-cluster cluster_rule_toggle map and the org-wide
// rule_disable map, alone and together.
func TestFilterDisabledRulesFromReport(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	testCases := []struct {
		name            string
		clusterDisabled types.ClusterDisabledRules
		orgDisabled     types.OrgDisabledRules
		expectedRuleIDs []string
		expectUnchanged bool
	}{
		{
			name:            "a rule disabled for the cluster is omitted, the active one is kept",
			clusterDisabled: disabledRulesForCluster(cluster, criticalImpactErrorKey),
			expectedRuleIDs: []string{importantRuleCompositeID},
		},
		{
			name:            "a rule acked for the organization is omitted, the active one is kept",
			orgDisabled:     disabledRulesForOrg("1", criticalImpactErrorKey),
			expectedRuleIDs: []string{importantRuleCompositeID},
		},
		{
			name:            "both maps are consulted, so a rule disabled in either one is omitted",
			clusterDisabled: disabledRulesForCluster(cluster, criticalImpactErrorKey),
			orgDisabled:     disabledRulesForOrg("1", importantImpactErrorKey),
			expectedRuleIDs: []string{},
		},
		{
			name:            "every rule disabled leaves an empty list of rule hits",
			clusterDisabled: disabledRulesForCluster(cluster, criticalImpactErrorKey, importantImpactErrorKey),
			expectedRuleIDs: []string{},
		},
		{
			name: "rules disabled for another cluster or another organization are not omitted",
			clusterDisabled: types.ClusterDisabledRules{
				{ClusterID: "other-cluster", RuleID: disabledRulesTestRuleID, ErrorKey: criticalImpactErrorKey}: {},
			},
			orgDisabled:     disabledRulesForOrg("999", importantImpactErrorKey),
			expectedRuleIDs: []string{criticalRuleCompositeID, importantRuleCompositeID},
			expectUnchanged: true,
		},
		{
			name:            "a disabled rule of another rule id does not omit anything",
			clusterDisabled: types.ClusterDisabledRules{{ClusterID: cluster.ClusterName, RuleID: "other_rule", ErrorKey: criticalImpactErrorKey}: {}},
			expectedRuleIDs: []string{criticalRuleCompositeID, importantRuleCompositeID},
			expectUnchanged: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := setupDisabledRulesLogCapture(t)

			d := differ.Differ{
				ClusterDisabledRules: tc.clusterDisabled,
				OrgDisabledRules:     tc.orgDisabled,
			}

			filtered := differ.FilterDisabledRulesFromReport(&d, cluster, storedReportTwoRulesJSON)

			assert.Equal(t, tc.expectedRuleIDs, storedReportRuleIDs(t, filtered))
			assert.True(t, storedReportHasReportsField(t, filtered),
				"the stored report must keep its list of rule hits, even when it is empty")
			assert.NotContains(t, buf.String(), differ.ReportFilteringFailedMessage)

			if tc.expectUnchanged {
				// nothing of this report is disabled, so it is stored as it is
				assert.EqualValues(t, storedReportTwoRulesJSON, filtered)
				assert.NotContains(t, buf.String(), differ.DisabledRulesOmittedMessage)
			} else {
				assert.NotEqualValues(t, storedReportTwoRulesJSON, filtered,
					"the report stored must not be the original one when a rule is omitted")
				assert.Contains(t, buf.String(), differ.DisabledRulesOmittedMessage)
			}
		})
	}
}

// TestFilterDisabledRulesFromReportWithIgnoreDisabledRulesFlag verifies that the
// report is stored unfiltered when no rule is disabled at all, which is what
// --ignore-disabled-rules amounts to: the aggregator database is never queried
// and both maps are left empty.
func TestFilterDisabledRulesFromReportWithIgnoreDisabledRulesFlag(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	testCases := []struct {
		name            string
		report          types.ClusterReport
		clusterDisabled types.ClusterDisabledRules
		orgDisabled     types.OrgDisabledRules
	}{
		{
			name:   "maps never allocated",
			report: storedReportTwoRulesJSON,
		},
		{
			name:            "maps allocated but empty",
			report:          storedReportTwoRulesJSON,
			clusterDisabled: make(types.ClusterDisabledRules),
			orgDisabled:     make(types.OrgDisabledRules),
		},
		{
			// With nothing to omit the report is never even looked at, so a
			// report that could not be processed as JSON is not reported as a
			// filtering failure either.
			name:   "report that is not valid JSON",
			report: "this is not JSON",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := setupDisabledRulesLogCapture(t)

			d := differ.Differ{
				IgnoreDisabledRules:  true,
				ClusterDisabledRules: tc.clusterDisabled,
				OrgDisabledRules:     tc.orgDisabled,
			}

			filtered := differ.FilterDisabledRulesFromReport(&d, cluster, tc.report)

			assert.Equal(t, tc.report, filtered,
				"the report must be stored unfiltered when no rule is disabled")

			executionLog := buf.String()
			assert.NotContains(t, executionLog, differ.DisabledRulesOmittedMessage)
			assert.NotContains(t, executionLog, differ.ReportFilteringFailedMessage,
				"the report must not even be processed when no rule is disabled")
		})
	}
}

// TestFilterDisabledRulesFromReportIdentifiesRulesByComponentAndKey verifies
// which part of a rule hit is matched against the aggregator tables, where
// rule_id ("test_rule") and error_key live in separate columns. The hit is
// identified by its "component" (through moduleToRuleName) and its "key", the
// same pair the composite "rule_id|error_key" field encodes. The composite field
// itself is only carried over untouched: types.ReportItem has no field mapped to
// it, so a hit whose composite identifier disagrees with its component and key
// is still matched and omitted.
func TestFilterDisabledRulesFromReportIdentifiesRulesByComponentAndKey(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	testCases := []struct {
		name            string
		report          types.ClusterReport
		expectedRuleIDs []string
	}{
		{
			name: "the hit whose component and key match the disabled rule is omitted",
			report: types.ClusterReport(fmt.Sprintf(`{"reports": [
				{"rule_id": %q, "component": %q, "type": "rule", "key": %q},
				{"rule_id": %q, "component": %q, "type": "rule", "key": %q}
			]}`,
				criticalRuleCompositeID, disabledRulesTestModule, criticalImpactErrorKey,
				importantRuleCompositeID, disabledRulesTestModule, importantImpactErrorKey)),
			expectedRuleIDs: []string{importantRuleCompositeID},
		},
		{
			name: "a composite rule_id disagreeing with the component and key is ignored",
			report: types.ClusterReport(fmt.Sprintf(`{"reports": [
				{"rule_id": "nope|NOPE", "component": %q, "type": "rule", "key": %q},
				{"rule_id": %q, "component": %q, "type": "rule", "key": %q}
			]}`,
				disabledRulesTestModule, criticalImpactErrorKey,
				criticalRuleCompositeID, disabledRulesTestModule, importantImpactErrorKey)),
			// the hit carrying the critical composite identifier is the one
			// kept here: its component and key say it is the important rule
			expectedRuleIDs: []string{criticalRuleCompositeID},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := differ.Differ{
				ClusterDisabledRules: disabledRulesForCluster(cluster, criticalImpactErrorKey),
			}

			filtered := differ.FilterDisabledRulesFromReport(&d, cluster, tc.report)

			assert.Equal(t, tc.expectedRuleIDs, storedReportRuleIDs(t, filtered))
		})
	}
}

// TestFilterDisabledRulesFromReportKeepsEverythingElseIntact verifies that
// omitting a disabled rule hit leaves the rest of the report document exactly as
// it was, including the parts this service does not model (the composite
// rule_id, tags and links of the remaining hits, and the top level
// analysis_metadata). Losing them would degrade the report stored in the
// `reported` table on every run.
func TestFilterDisabledRulesFromReportKeepsEverythingElseIntact(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	d := differ.Differ{
		ClusterDisabledRules: disabledRulesForCluster(cluster, criticalImpactErrorKey),
	}

	filtered := differ.FilterDisabledRulesFromReport(&d, cluster, storedReportTwoRulesJSON)

	var document struct {
		AnalysisMetadata map[string]string `json:"analysis_metadata"`
		Reports          []map[string]any  `json:"reports"`
	}
	err := json.Unmarshal([]byte(filtered), &document)
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{"metadata": "some metadata"}, document.AnalysisMetadata,
		"the top level metadata of the report must survive the filtering")

	if assert.Len(t, document.Reports, 1, "only the active rule hit must be left") {
		assert.Equal(t, map[string]any{
			"rule_id":   importantRuleCompositeID,
			"component": string(disabledRulesTestModule),
			"type":      "rule",
			"key":       string(importantImpactErrorKey),
			"details":   map[string]any{"detail": "important details"},
			"tags":      []any{"openshift"},
			"links":     map[string]any{"docs": []any{"https://example.com/important"}},
		}, document.Reports[0], "the kept rule hit must be carried over untouched")
	}
}

// TestFilterDisabledRulesFromReportWithUnprocessableReport verifies that a
// report that cannot be processed as JSON is stored as it is rather than lost or
// replaced by something invalid, and that the failure is logged. A rule hit that
// cannot be identified is kept for the same reason: it cannot be matched against
// the disabled rules either.
func TestFilterDisabledRulesFromReportWithUnprocessableReport(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	disabledHit := fmt.Sprintf(`{"rule_id": %q, "component": %q, "type": "rule", "key": %q}`,
		criticalRuleCompositeID, disabledRulesTestModule, criticalImpactErrorKey)

	testCases := []struct {
		name             string
		report           types.ClusterReport
		expectedReport   types.ClusterReport
		expectFailureLog bool
	}{
		{
			name:             "report that is not valid JSON",
			report:           "this is not JSON",
			expectedReport:   "this is not JSON",
			expectFailureLog: true,
		},
		{
			name:           "report without any list of rule hits",
			report:         `{"analysis_metadata": {"metadata": "some metadata"}}`,
			expectedReport: `{"analysis_metadata": {"metadata": "some metadata"}}`,
		},
		{
			name:             "list of rule hits that is not a list",
			report:           `{"reports": {"not": "a list"}}`,
			expectedReport:   `{"reports": {"not": "a list"}}`,
			expectFailureLog: true,
		},
		{
			name:             "rule hit that cannot be identified is kept, the disabled one is still omitted",
			report:           types.ClusterReport(fmt.Sprintf(`{"reports": ["not a rule hit", %s]}`, disabledHit)),
			expectedReport:   `{"reports": ["not a rule hit"]}`,
			expectFailureLog: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := setupDisabledRulesLogCapture(t)

			d := differ.Differ{
				ClusterDisabledRules: disabledRulesForCluster(cluster, criticalImpactErrorKey),
			}

			filtered := differ.FilterDisabledRulesFromReport(&d, cluster, tc.report)

			if json.Valid([]byte(tc.report)) {
				assert.JSONEq(t, string(tc.expectedReport), string(filtered))
			} else {
				assert.Equal(t, tc.expectedReport, filtered, "a report that is not JSON must be stored as it is")
			}
			assert.Equal(t, tc.expectFailureLog, strings.Contains(buf.String(), differ.ReportFilteringFailedMessage),
				"unexpected presence or absence of the report filtering failure log")
		})
	}
}

// TestProduceEntriesToKafkaStoresReportWithoutDisabledRules verifies that the
// Kafka path writes the report without the disabled rules to the `reported`
// table, whatever the outcome of the cluster is: a notification sent (state
// "sent"), no new issue to notify (state "same") or a delivery failure (state
// "error"). The stored report is the baseline the next runs compare against, so
// a re-enabled rule must not be found in it.
func TestProduceEntriesToKafkaStoresReportWithoutDisabledRules(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	testCases := []struct {
		name             string
		clusterDisabled  types.ClusterDisabledRules
		produceError     error
		expectedNotified int
		expectedRuleIDs  []string
		expectUnchanged  bool
	}{
		{
			name:             "notification sent, the disabled rule is omitted from the stored report",
			clusterDisabled:  disabledRulesForCluster(cluster, criticalImpactErrorKey),
			expectedNotified: 1,
			expectedRuleIDs:  []string{importantRuleCompositeID},
		},
		{
			name:             "nothing to notify, every rule disabled, an empty list of rule hits is stored",
			clusterDisabled:  disabledRulesForCluster(cluster, criticalImpactErrorKey, importantImpactErrorKey),
			expectedNotified: 0,
			expectedRuleIDs:  []string{},
		},
		{
			name:             "delivery failure, the disabled rule is omitted from the stored report",
			clusterDisabled:  disabledRulesForCluster(cluster, criticalImpactErrorKey),
			produceError:     fmt.Errorf("kafka is down"),
			expectedNotified: -1,
			expectedRuleIDs:  []string{importantRuleCompositeID},
		},
		{
			name:             "no rule disabled, the report is stored unfiltered",
			expectedNotified: 2,
			expectedRuleIDs:  []string{criticalRuleCompositeID, importantRuleCompositeID},
			expectUnchanged:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var storedReports []types.ClusterReport
			storage := newStoredReportStorageMock(&storedReports)

			producerMock := &mocks.Producer{}
			producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).
				Return(int32(0), int64(1), tc.produceError)

			d := differ.Differ{
				Storage:  storage,
				Notifier: producerMock,
				Filter:   differ.DefaultEventFilter,
				Thresholds: differ.EventThresholds{
					TotalRisk: differ.DefaultTotalRiskThreshold,
				},
				ClusterDisabledRules: tc.clusterDisabled,
			}

			notified, err := differ.ProduceEntriesToKafka(
				&d, cluster, disabledRulesTestRuleContent(),
				storedReportTestReportItems(t), storedReportTwoRulesJSON)

			if tc.produceError != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectedNotified, notified)

			if assert.Len(t, storedReports, 1, "exactly one record must be written for the cluster") {
				assert.Equal(t, tc.expectedRuleIDs, storedReportRuleIDs(t, storedReports[0]))
				assert.True(t, storedReportHasReportsField(t, storedReports[0]),
					"the stored report must keep its list of rule hits, even when it is empty")
				if tc.expectUnchanged {
					assert.EqualValues(t, storedReportTwoRulesJSON, storedReports[0])
				}
			}
		})
	}
}

// newStoredReportRendererServer returns a template renderer stub rendering both
// rule hits of the shared report fixture for the given cluster.
func newStoredReportRendererServer(t *testing.T, cluster types.ClusterEntry) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprintf(w, `{"clusters":[%q],"reports":{%q:[
			{"rule_id":%q,"error_key":%q,"resolution":"resolution","reason":"reason","description":%q},
			{"rule_id":%q,"error_key":%q,"resolution":"resolution","reason":"reason","description":%q}
		]}}`,
			cluster.ClusterName, cluster.ClusterName,
			disabledRulesTestRuleID, criticalImpactErrorKey, criticalRuleDescription,
			disabledRulesTestRuleID, importantImpactErrorKey, importantRuleDescription)
		assert.NoError(t, err)
	}))
}

// TestProcessReportsByClusterStoresReportWithoutDisabledRulesForServiceLog
// verifies that the Service Log path also writes the report without the
// disabled rules to the `reported` table. The filtering cannot happen inside
// ProduceEntriesToServiceLog, which never sees the report, so this goes through
// processReportsByCluster, the shared loop that reads the report and updates the
// notification record.
func TestProcessReportsByClusterStoresReportWithoutDisabledRulesForServiceLog(t *testing.T) {
	cluster := disabledRulesTestClusterEntry()

	testCases := []struct {
		name                    string
		clusterDisabled         types.ClusterDisabledRules
		orgDisabled             types.OrgDisabledRules
		expectedServiceLogCalls int
		expectedRuleIDs         []string
		expectUnchanged         bool
	}{
		{
			name:                    "entry sent, the rule disabled for the cluster is omitted from the stored report",
			clusterDisabled:         disabledRulesForCluster(cluster, criticalImpactErrorKey),
			expectedServiceLogCalls: 1,
			expectedRuleIDs:         []string{importantRuleCompositeID},
		},
		{
			name:                    "nothing sent, every rule acked for the organization, an empty list of rule hits is stored",
			orgDisabled:             disabledRulesForOrg("1", criticalImpactErrorKey, importantImpactErrorKey),
			expectedServiceLogCalls: 0,
			expectedRuleIDs:         []string{},
		},
		{
			name:                    "no rule disabled, the report is stored unfiltered",
			expectedServiceLogCalls: 2,
			expectedRuleIDs:         []string{criticalRuleCompositeID, importantRuleCompositeID},
			expectUnchanged:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := newStoredReportRendererServer(t, cluster)
			defer server.Close()

			config := conf.ConfigStruct{
				ServiceLog: conf.ServiceLogConfiguration{
					Enabled: true,
				},
				Dependencies: conf.DependenciesConfiguration{
					TemplateRendererServer:   server.URL,
					TemplateRendererEndpoint: "/rendered_reports",
					TemplateRendererURL:      server.URL + "/rendered_reports",
				},
			}

			var storedReports []types.ClusterReport
			storage := newStoredReportStorageMock(&storedReports)
			storage.On("ReadReportForClusterAtTime",
				mock.AnythingOfType("types.OrgID"),
				mock.AnythingOfType("types.ClusterName"),
				mock.AnythingOfType("types.Timestamp")).
				Return(types.ClusterReport(storedReportTwoRulesJSON), nil)

			producerMock := &mocks.Producer{}
			producerMock.On("ProduceMessage", mock.AnythingOfType("types.ProducerMessage")).
				Return(int32(0), int64(1), nil)

			d := differ.Differ{
				Storage:  storage,
				Notifier: producerMock,
				Target:   types.ServiceLogTarget,
				Filter:   differ.DefaultEventFilter,
				Thresholds: differ.EventThresholds{
					TotalRisk: differ.DefaultTotalRiskThreshold,
				},
				ClusterDisabledRules: tc.clusterDisabled,
				OrgDisabledRules:     tc.orgDisabled,
			}

			differ.ProcessReportsByCluster(&d, &config, disabledRulesTestRuleContent(), []types.ClusterEntry{cluster})

			producerMock.AssertNumberOfCalls(t, "ProduceMessage", tc.expectedServiceLogCalls)

			if assert.Len(t, storedReports, 1, "exactly one record must be written for the cluster") {
				assert.Equal(t, tc.expectedRuleIDs, storedReportRuleIDs(t, storedReports[0]))
				assert.True(t, storedReportHasReportsField(t, storedReports[0]),
					"the stored report must keep its list of rule hits, even when it is empty")
				if tc.expectUnchanged {
					assert.EqualValues(t, storedReportTwoRulesJSON, storedReports[0])
				}
			}
		})
	}
}
