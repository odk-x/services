/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.sync.service.data;

import java.util.ArrayList;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.OrderedColumns;

/**
 * Tracks the data values for the local and server row so that we
 * can construct the appropriate database insert/update statements.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public final class SyncRowDataChanges {
  public final SyncRow serverRow;
  public final SyncRow localRow;
  public final int localRowConflictType;
  // this can be modified during the sync process
  public boolean isSyncedPendingFiles;

  public SyncRowDataChanges(SyncRow serverRow, SyncRow localRow, boolean isSyncedPendingFiles) {
    this.serverRow = serverRow;
    this.localRow = localRow;
    this.isSyncedPendingFiles = isSyncedPendingFiles;
    this.localRowConflictType = -1;
  }

  public SyncRowDataChanges(SyncRow serverRow, SyncRow localRow, boolean isSyncedPendingFiles,
      int localRowConflictType) {
    this.serverRow = serverRow;
    this.localRow = localRow;
    this.isSyncedPendingFiles = isSyncedPendingFiles;
    this.localRowConflictType = localRowConflictType;
  }

  /**
   * Compares user and metadata field values, but excludes sync_state, rowETag,
   * filter type and filter value.
   *
   * @param orderedDefns
   * @return true if the fields are identical
     */
  public boolean identicalValuesExceptRowETagAndFilterScope(OrderedColumns orderedDefns) {
    if ((serverRow.getSavepointTimestamp() == null) ? (localRow.getSavepointTimestamp() != null)
        : !serverRow.getSavepointTimestamp().equals(localRow.getSavepointTimestamp())) {
      return false;
    }
    if ((serverRow.getSavepointCreator() == null) ? (localRow.getSavepointCreator() != null)
        : !serverRow.getSavepointCreator().equals(localRow.getSavepointCreator())) {
      return false;
    }
    if ((serverRow.getFormId() == null) ? (localRow.getFormId() != null) : !serverRow.getFormId()
        .equals(localRow.getFormId())) {
      return false;
    }
    if ((serverRow.getLocale() == null) ? (localRow.getLocale() != null) : !serverRow.getLocale()
        .equals(localRow.getLocale())) {
      return false;
    }
    if ((serverRow.getRowId() == null) ? (localRow.getRowId() != null) : !serverRow.getRowId()
        .equals(localRow.getRowId())) {
      return false;
    }
    if ((serverRow.getSavepointType() == null) ? (localRow.getSavepointType() != null)
        : !serverRow.getSavepointType().equals(localRow.getSavepointType())) {
      return false;
    }
    ArrayList<DataKeyValue> localValues = localRow.getValues();
    ArrayList<DataKeyValue> serverValues = serverRow.getValues();

    if (localValues == null && serverValues == null) {
      return true;
    } else if (localValues == null || serverValues == null) {
      return false;
    }

    if (localValues.size() != serverValues.size()) {
      return false;
    }

    for (int i = 0; i < localValues.size(); ++i) {
      DataKeyValue local = localValues.get(i);
      DataKeyValue server = serverValues.get(i);
      if (!local.column.equals(server.column)) {
        return false;
      }
      if (local.value == null && server.value == null) {
        continue;
      } else if (local.value == null || server.value == null) {
        return false;
      } else if (local.value.equals(server.value)) {
        continue;
      }

      // NOT textually identical.
      //
      // Everything must be textually identical except possibly number fields
      // which may have rounding due to different database implementations,
      // data representations, and marshaling libraries.
      //
      ColumnDefinition cd = orderedDefns.find(local.column);
      if (cd.getType().getDataType() == ElementDataType.number) {
        // !!Important!! Double.valueOf(str) handles NaN and +/-Infinity
        Double localNumber = Double.valueOf(local.value);
        Double serverNumber = Double.valueOf(server.value);

        if (localNumber.equals(serverNumber)) {
          // simple case -- trailing zeros or string representation mix-up
          //
          continue;
        } else if (localNumber.isInfinite() && serverNumber.isInfinite()) {
          // if they are both plus or both minus infinity, we have a match
          if (Math.signum(localNumber) == Math.signum(serverNumber)) {
            continue;
          } else {
            return false;
          }
        } else if (localNumber.isNaN() || localNumber.isInfinite() || serverNumber.isNaN()
            || serverNumber.isInfinite()) {
          // one or the other is special1
          return false;
        } else {
          double localDbl = localNumber;
          double serverDbl = serverNumber;
          if (localDbl == serverDbl) {
            continue;
          }
          // OK. We have two values like 9.80 and 9.8
          // consider them equal if they are adjacent to each other.
          double localNear = localDbl;
          int idist = 0;
          int idistMax = 128;
          for (idist = 0; idist < idistMax; ++idist) {
            localNear = Math.nextAfter(localNear, serverDbl);
            if (localNear == serverDbl) {
              break;
            }
          }
          if (idist < idistMax) {
            continue;
          }
          return false;
        }
      } else {
        // textual identity is required!
        return false;
      }
    }
    // because we ensure that (localValues.size() == serverValues.size())
    // and we ensure inside the loop that (local.column.equals(server.column))
    // and we test for semantic equivalence of the fields (handling numbers specially)
    // we should NOT test for containment of the server field-value list in the
    // local field-value list.  That is too restrictive because it rejects
    // semantically-equivalent field-value entries (which is the entire point of
    // the above loop).
    return true;
  }

  /**
   * Compares user and metadata values, excluding the sync state.
   *
   * @param orderedDefns
   * @return
     */
  public boolean identicalValues(OrderedColumns orderedDefns) {
    if ((serverRow.getRowFilterScope() == null) ? (localRow.getRowFilterScope() != null) : !serverRow
        .getRowFilterScope().equals(localRow.getRowFilterScope())) {
      return false;
    }
    if ((serverRow.getRowETag() == null) ? (localRow.getRowETag() != null) : !serverRow
        .getRowETag().equals(localRow.getRowETag())) {
      return false;
    }
    return identicalValuesExceptRowETagAndFilterScope(orderedDefns);
  }
}