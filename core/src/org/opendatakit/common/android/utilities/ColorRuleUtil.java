/*
 * Copyright (C) 2012 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.common.android.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.common.android.data.ColorRule;
import org.opendatakit.common.android.provider.DataTableColumns;

import android.graphics.Color;

/**
 *
 * @author sudar.sam@gmail.com
 *
 */
public class ColorRuleUtil {

  private static final String ID_SYNCED_RULE = "syncStateSynced";
  
  private static final String ID_IN_CONFLICT_RULE =
      "defaultRule_syncStateInConflict";
  private static final String ID_NEW_ROW_RULE =
      "defaultRule_syncStateNewRow";
  private static final String ID_CHANGED_RULE =
      "defaultRule_syncStateChanged";
  private static final String ID_DELETED_RULE =
      "defaultRule_syncStateDeleted";

  private static final int DEFAULT_SYNC_STATE_SYNCED_FOREGROUND = Color.BLACK;
  private static final int DEFAULT_SYNC_STATE_SYNCED_BACKGROUND = Color.WHITE;

  private static final int DEFAULT_SYNC_STATE_NEW_ROW_FOREGROUND =
      Color.BLACK;
  private static final int DEFAULT_SYNC_STATE_NEW_ROW_BACKGROUND =
      Color.GREEN;

  private static final int DEFAULT_SYNC_STATE_CHANGED_FOREGROUND =
      Color.BLACK;
  private static final int DEFAULT_SYNC_STATE_CHANGED_BACKGROUND =
	  0xfff1b82d; // http://identity.missouri.edu/logos-design/mu-colors.php

  private static final int DEFAULT_SYNC_STATE_IN_CONFLICT_FOREGROUND =
      Color.BLACK;
  private static final int DEFAULT_SYNC_STATE_IN_CONFLICT_BACKGROUND =
      Color.RED;

  private static final int DEFAULT_SYNC_STATE_DELETED_FOREGROUND =
      Color.BLACK;
  private static final int DEFAULT_SYNC_STATE_DELETED_BACKGROUND =
      Color.DKGRAY;

  private static final List<ColorRule> defaultSyncStateColorRules;
  private static final Set<String> defaultSyncStateColorRuleIDs;

  static {
    List<ColorRule> ruleList = new ArrayList<ColorRule>();
    ruleList.add(getColorRuleForSyncStateSynced());
    ruleList.add(getColorRuleForSyncStateNewRow());
    ruleList.add(getColorRuleForSyncStateChanged());
    ruleList.add(getColorRuleForSyncStateInConflict());
    ruleList.add(getColorRuleForSyncStateDeleted());
    defaultSyncStateColorRules = Collections.unmodifiableList(ruleList);
    // Now the rule ID set.
    Set<String> idSet = new HashSet<String>();
    idSet.add(ID_SYNCED_RULE);
    idSet.add(ID_IN_CONFLICT_RULE);
    idSet.add(ID_DELETED_RULE);
    idSet.add(ID_NEW_ROW_RULE);
    idSet.add(ID_CHANGED_RULE);
    defaultSyncStateColorRuleIDs = Collections.unmodifiableSet(idSet);
  }

  private static ColorRule getColorRuleForSyncStateSynced() {
    return new ColorRule(ID_SYNCED_RULE, DataTableColumns.SYNC_STATE,
        ColorRule.RuleType.EQUAL, SyncState.synced.name(),
        DEFAULT_SYNC_STATE_SYNCED_FOREGROUND,
        DEFAULT_SYNC_STATE_SYNCED_BACKGROUND);
  }

  private static ColorRule getColorRuleForSyncStateNewRow() {
    return new ColorRule(ID_NEW_ROW_RULE, DataTableColumns.SYNC_STATE,
        ColorRule.RuleType.EQUAL, SyncState.new_row.name(),
        DEFAULT_SYNC_STATE_NEW_ROW_FOREGROUND,
        DEFAULT_SYNC_STATE_NEW_ROW_BACKGROUND);
  }

  private static ColorRule getColorRuleForSyncStateChanged() {
    return new ColorRule(ID_CHANGED_RULE, DataTableColumns.SYNC_STATE,
        ColorRule.RuleType.EQUAL, SyncState.changed.name(),
        DEFAULT_SYNC_STATE_CHANGED_FOREGROUND,
        DEFAULT_SYNC_STATE_CHANGED_BACKGROUND);
  }

  private static ColorRule getColorRuleForSyncStateDeleted() {
    return new ColorRule(ID_DELETED_RULE, DataTableColumns.SYNC_STATE,
        ColorRule.RuleType.EQUAL, SyncState.deleted.name(),
        DEFAULT_SYNC_STATE_DELETED_FOREGROUND,
        DEFAULT_SYNC_STATE_DELETED_BACKGROUND);
  }

  private static ColorRule getColorRuleForSyncStateInConflict() {
    return new ColorRule(ID_IN_CONFLICT_RULE, DataTableColumns.SYNC_STATE,
        ColorRule.RuleType.EQUAL, SyncState.in_conflict.name(),
        DEFAULT_SYNC_STATE_IN_CONFLICT_FOREGROUND,
        DEFAULT_SYNC_STATE_IN_CONFLICT_BACKGROUND);
  }

  /**
   * Get an unmodifiable list of the default color rules for the various sync
   * states.
   * @return
   */
  public static List<ColorRule> getDefaultSyncStateColorRules() {
    return defaultSyncStateColorRules;
  }

  /**
   * Get an unmodifiable set of the default sync state color rule ids.
   * @return
   */
  public static Set<String> getDefaultSyncStateColorRuleIds() {
    return defaultSyncStateColorRuleIDs;
  }

}
