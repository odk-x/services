package org.opendatakit.utilities.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.data.ColorRule;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.utilities.ColorRuleUtil;

import java.util.List;
import java.util.Set;

/**
 * Tests for ColorRuleUtil class which provides default color rules for different sync states.
 */
public class ColorRuleUtilTest {

    @Test
    public void testGetDefaultSyncStateColorRules() {
        // Get the default rules list
        List<ColorRule> defaultRules = ColorRuleUtil.getDefaultSyncStateColorRules();

        // Verify basic properties
        assertNotNull("Default sync state color rules should not be null", defaultRules);
        assertFalse("Default sync state color rules should not be empty", defaultRules.isEmpty());
        assertEquals("Should have exactly 5 default sync state rules", 5, defaultRules.size());

        // Verify the rules contain expected sync states
        boolean hasSynced = false;
        boolean hasNewRow = false;
        boolean hasChanged = false;
        boolean hasInConflict = false;
        boolean hasDeleted = false;

        for (ColorRule rule : defaultRules) {
            assertNotNull("Rule should not be null", rule);
            assertEquals("Column should be SYNC_STATE", DataTableColumns.SYNC_STATE, rule.getColumnElementKey());
            assertEquals("Rule type should be EQUAL", ColorRule.RuleType.EQUAL, rule.getOperator());

            String value = rule.getVal();
            if (SyncState.synced.name().equals(value)) {
                hasSynced = true;
            } else if (SyncState.new_row.name().equals(value)) {
                hasNewRow = true;
            } else if (SyncState.changed.name().equals(value)) {
                hasChanged = true;
            } else if (SyncState.in_conflict.name().equals(value)) {
                hasInConflict = true;
            } else if (SyncState.deleted.name().equals(value)) {
                hasDeleted = true;
            }
        }

        assertTrue("Should have a rule for synced state", hasSynced);
        assertTrue("Should have a rule for new_row state", hasNewRow);
        assertTrue("Should have a rule for changed state", hasChanged);
        assertTrue("Should have a rule for in_conflict state", hasInConflict);
        assertTrue("Should have a rule for deleted state", hasDeleted);
    }

    @Test
    public void testRulesAreUnmodifiable() {
        // Verify that the collection returned is unmodifiable
        List<ColorRule> defaultRules = ColorRuleUtil.getDefaultSyncStateColorRules();

        boolean isUnmodifiable = false;
        try {
            defaultRules.clear();
        } catch (UnsupportedOperationException e) {
            isUnmodifiable = true;
        }
        assertTrue("Default rules list should be unmodifiable", isUnmodifiable);
    }

    @Test
    public void testGetDefaultSyncStateColorRuleIds() {
        // Get the default rule IDs set
        Set<String> defaultRuleIds = ColorRuleUtil.getDefaultSyncStateColorRuleIds();

        // Verify basic properties
        assertNotNull("Default sync state color rule IDs should not be null", defaultRuleIds);
        assertFalse("Default sync state color rule IDs should not be empty", defaultRuleIds.isEmpty());
        assertEquals("Should have exactly 5 default sync state rule IDs", 5, defaultRuleIds.size());

        // Verify specific IDs
        assertTrue("Should contain ID for synced rule", defaultRuleIds.contains("syncStateSynced"));
        assertTrue("Should contain ID for new row rule", defaultRuleIds.contains("defaultRule_syncStateNewRow"));
        assertTrue("Should contain ID for changed rule", defaultRuleIds.contains("defaultRule_syncStateChanged"));
        assertTrue("Should contain ID for in conflict rule", defaultRuleIds.contains("defaultRule_syncStateInConflict"));
        assertTrue("Should contain ID for deleted rule", defaultRuleIds.contains("defaultRule_syncStateDeleted"));
    }

    @Test
    public void testRuleIdsAreUnmodifiable() {
        // Verify that the collection returned is unmodifiable
        Set<String> defaultRuleIds = ColorRuleUtil.getDefaultSyncStateColorRuleIds();

        boolean isUnmodifiable = false;
        try {
            defaultRuleIds.clear();
        } catch (UnsupportedOperationException e) {
            isUnmodifiable = true;
        }
        assertTrue("Default rule IDs set should be unmodifiable", isUnmodifiable);
    }
}
