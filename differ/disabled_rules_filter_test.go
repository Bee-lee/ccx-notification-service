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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RedHatInsights/ccx-notification-service/differ"
	"github.com/RedHatInsights/ccx-notification-service/types"
)

// --- moduleToRuleID tests ---

// TestModuleToRuleIDStripsReportSuffix verifies that the .report suffix is
// removed from a fully qualified module name.
func TestModuleToRuleIDStripsReportSuffix(t *testing.T) {
	module := types.ModuleName("ccx_rules_ocp.external.rules.cluster_wide_proxy_auth_check.report")
	expected := types.RuleID("ccx_rules_ocp.external.rules.cluster_wide_proxy_auth_check")
	assert.Equal(t, expected, differ.ModuleToRuleID(module))
}

// TestModuleToRuleIDNoReportSuffix verifies that a module name without .report
// is returned unchanged.
func TestModuleToRuleIDNoReportSuffix(t *testing.T) {
	module := types.ModuleName("ccx_rules_ocp.external.rules.some_rule")
	expected := types.RuleID("ccx_rules_ocp.external.rules.some_rule")
	assert.Equal(t, expected, differ.ModuleToRuleID(module))
}

// --- isRuleDisabled tests ---

// TestIsRuleDisabledClusterLevel verifies that a rule present in the
// cluster-level disabled map (cluster_rule_toggle) is correctly identified as
// disabled.
func TestIsRuleDisabledClusterLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				RuleID:    "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey:  "TEST_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledOrgLevel verifies that a rule present in the org-level
// disabled map (rule_disable) is correctly identified as disabled.
func TestIsRuleDisabledOrgLevel(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       42,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "42",
				RuleID:   "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey: "TEST_ERROR_KEY",
			}: {},
		},
	}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledNotInEitherMap verifies that a rule not present in either
// disabled map is not considered disabled.
func TestIsRuleDisabledNotInEitherMap(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules:     make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledDifferentErrorKeyNotMatched verifies that a rule with the
// same rule ID but a different error key is not considered disabled.
func TestIsRuleDisabledDifferentErrorKeyNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("DIFFERENT_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				RuleID:    "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey:  "TEST_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledDifferentClusterNotMatched verifies that a rule disabled
// for one cluster is not considered disabled for a different cluster.
func TestIsRuleDisabledDifferentClusterNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       1,
		ClusterName: "11111111-2222-3333-4444-555555555555",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				RuleID:    "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey:  "TEST_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: make(types.OrgDisabledRules),
	}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledDifferentOrgNotMatched verifies that a rule disabled for
// one org is not considered disabled for a different org.
func TestIsRuleDisabledDifferentOrgNotMatched(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       99,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: make(types.ClusterDisabledRules),
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "42",
				RuleID:   "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey: "TEST_ERROR_KEY",
			}: {},
		},
	}

	assert.False(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}

// TestIsRuleDisabledClusterLevelTakesPrecedence verifies that when a rule is
// present in both maps, it is still correctly identified as disabled (cluster
// level is checked first).
func TestIsRuleDisabledClusterLevelTakesPrecedence(t *testing.T) {
	cluster := types.ClusterEntry{
		OrgID:       42,
		ClusterName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	module := types.ModuleName("ccx_rules_ocp.external.rules.test_rule.report")
	errorKey := types.ErrorKey("TEST_ERROR_KEY")

	d := differ.Differ{
		ClusterDisabledRules: types.ClusterDisabledRules{
			{
				ClusterID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				RuleID:    "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey:  "TEST_ERROR_KEY",
			}: {},
		},
		OrgDisabledRules: types.OrgDisabledRules{
			{
				OrgID:    "42",
				RuleID:   "ccx_rules_ocp.external.rules.test_rule",
				ErrorKey: "TEST_ERROR_KEY",
			}: {},
		},
	}

	assert.True(t, differ.IsRuleDisabled(&d, cluster, module, errorKey))
}
