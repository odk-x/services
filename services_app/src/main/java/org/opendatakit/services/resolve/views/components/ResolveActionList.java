/*
 * Copyright (C) 2015 University of Washington
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
package org.opendatakit.services.resolve.views.components;

import org.opendatakit.aggregate.odktables.rest.ConflictType;

import java.util.ArrayList;

/**
 * @author mitchellsundt@gmail.com
 */
public final class ResolveActionList {

  public final Integer localConflictType;
  public final Integer serverConflictType;
  public final ResolveActionType actionType;
  public final ArrayList<ConcordantColumn> concordantColumns;
  public final ArrayList<ConflictColumn> conflictColumns;

  /**
   * Checkpoints (and conflict rows) might only update metadata.
   * In that case, we should silently remove (and resolve) them.
   *
   * @return true if we should silently remove or resolve this item.
   */
  public boolean noChangesInUserDefinedFieldValues() {
    return conflictColumns.isEmpty() &&
        (((actionType != null) && (actionType != ResolveActionType.DELETE)) ||
         ((actionType == null) &&
            ((localConflictType == ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
              serverConflictType == ConflictType.SERVER_UPDATED_UPDATED_VALUES) ||
             (localConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES &&
              serverConflictType == ConflictType.SERVER_DELETED_OLD_VALUES))));
  }

  /**
   * @return true if the apply-field-level-deltas button should not be displayed
   */
  public boolean hideDeltasButton() {
    return conflictColumns.isEmpty();
  }

  public ResolveActionList(ResolveActionType actionType,
      ArrayList<ConcordantColumn> concordantColumns,
      ArrayList<ConflictColumn> conflictColumns) {
    this.localConflictType = null;
    this.serverConflictType = null;
    this.actionType = actionType;
    this.concordantColumns = concordantColumns;
    this.conflictColumns = conflictColumns;
  }

  public ResolveActionList(int localConflictType, int serverConflictType,
      ArrayList<ConcordantColumn> concordantColumns,
      ArrayList<ConflictColumn> conflictColumns) {
    this.localConflictType = localConflictType;
    this.serverConflictType = serverConflictType;
    this.actionType = null;
    this.concordantColumns = concordantColumns;
    this.conflictColumns = conflictColumns;
  }
}
