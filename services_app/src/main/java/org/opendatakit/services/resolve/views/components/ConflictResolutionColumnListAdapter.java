/*
 * Copyright (C) 2014 University of Washington
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

import android.content.Context;
import android.util.SparseArray;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.LinearLayout;
import android.widget.RadioButton;
import android.widget.RelativeLayout;
import android.widget.TextView;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;

import java.util.ArrayList;

/**
 * A basic adapter for displaying things with sections. Based on the code from
 * the Google IO 2012 app:
 *
 * https://code.google.com/p/iosched/
 *
 * @author sudar.sam@gmail.com
 *
 */
public class ConflictResolutionColumnListAdapter extends BaseAdapter {

  private static final String TAG = ConflictResolutionColumnListAdapter.class.getSimpleName();

  private final String mAppName;
  private final int mLocalButtonTextId;
  private final int mServerButtonTextId;
  private UICallbacks mCallbacks;
  private LayoutInflater mLayoutInflater;
  private ArrayList<ConflictColumn> mConflictColumnArray = new ArrayList<ConflictColumn>();
  private SparseArray<ConflictColumn> mConflictColumns = new SparseArray<ConflictColumn>();
  private SparseArray<ConcordantColumn> mConcordantColumns = new SparseArray<ConcordantColumn>();

  public interface UICallbacks {

    /**
     * Store the user's selection so that it will be persisted across screen rotations.
     * @param elementKey
     * @param resolution
     * @param value
     */
    void onConflictResolutionDecision(String elementKey,
        Resolution resolution, String value);

    /**
     * Retrieve any preselected conflict resolution for this field.
     *
     * @param elementKey
     * @return null if a decision has not yet been made
     */
    Resolution getConflictResolutionDecision(String elementKey);

    /**
     * Called when the user has made a decision about which row to use.
     */
    void onDecisionMade();
  }

  /**
   * This is the padding on the left side of the text view for those items in
   * the adapter that aren't in a section.
   */
  private int mLeftPaddingOnTopLevel = -1;

  public ConflictResolutionColumnListAdapter(Context context, String appName,
      int localButtonTextId, int serverButtonTextId, UICallbacks callbacks) {
    this.mAppName = appName;
    this.mLocalButtonTextId = localButtonTextId;
    this.mServerButtonTextId = serverButtonTextId;
    this.mLayoutInflater = (LayoutInflater) context
        .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
    this.mCallbacks = callbacks;
  }

  public void clear() {
    mConcordantColumns.clear();
    mConflictColumns.clear();
    mConflictColumnArray.clear();
  }

  public void addAll(ResolveActionList resolveActionList) {
    for (ConcordantColumn cc : resolveActionList.concordantColumns) {
      mConcordantColumns.append(cc.getPosition(), cc);
    }
    for (ConflictColumn cc : resolveActionList.conflictColumns) {
      mConflictColumns.append(cc.getPosition(), cc);
    }
    mConflictColumnArray = resolveActionList.conflictColumns;
  }

  public int getConflictCount() {
    return mConflictColumns.size();
  }

  public ArrayList<ConflictColumn> getConflictColumnArray() {
    return mConflictColumnArray;
  }

  public boolean isConflictColumnPosition(int position) {
    return mConflictColumns.get(position) != null;
  }

  public boolean isConcordantColumnPosition(int position) {
    return mConcordantColumns.get(position) != null;
  }

  @Override
  public int getCount() {
    return (mConflictColumns.size() + mConcordantColumns.size());
  }

  @Override
  public Object getItem(int position) {
    // This position can be one of two types: a concordant, or a conflict column.
    if (isConflictColumnPosition(position)) {
      return mConflictColumns.get(position);
    } else if (isConcordantColumnPosition(position)) {
      return mConcordantColumns.get(position);
    } else {
      WebLogger.getLogger(mAppName).e(TAG,
          "[getItem] position " + position + " didn't match any of " + "the types!");
      return null;
    }
  }

  @Override
  public long getItemId(int position) {
    return Integer.MAX_VALUE - position;
  }

  @Override
  public int getItemViewType(int position) {
    if (isConflictColumnPosition(position)) {
      return 0;
    } else if (isConcordantColumnPosition(position)) {
      return 1;
    } else {
      WebLogger.getLogger(mAppName).e(TAG,
          "[getItem] position " + position + " didn't match any of " + "the types!");
      return -1;
    }
  }

  @Override
  public boolean isEnabled(int position) {
    boolean outcome = isConflictColumnPosition(position);
    return outcome;
  }

  @Override
  public int getViewTypeCount() {
    return 2; // conflict, concordant.
  }

  @Override
  public boolean areAllItemsEnabled() {
    // this might be false because this says in the spec something about
    // dividers returning true or false? Kind of a strange thing, but if
    // you're wondering why, consider looking at that.
    return false;
  }

  /**
   * Update the adapter's internal data structures to reflect the user's
   * choices.
   *
   * @param conflictColumn
   * @param decision
   */
  private void setResolution(ConflictColumn conflictColumn, Resolution decision) {
    String chosenValue = null;
    // we didn't return, so we know it's safe.
    if (decision == Resolution.LOCAL) {
      chosenValue = conflictColumn.getLocalRawValue();
    } else if (decision == Resolution.SERVER) {
      chosenValue = conflictColumn.getServerRawValue();
    }

    this.mCallbacks.onConflictResolutionDecision(conflictColumn.getElementKey(), decision,
        chosenValue);
  }

  @Override
  public View getView(int position, View convertView, ViewGroup parent) {
    if (isConflictColumnPosition(position)) {
      ConflictColumn conflictColumn = this.mConflictColumns.get(position);
      int layoutId = R.layout.list_item_conflict_row;
      LinearLayout view = (LinearLayout) convertView;
      if (convertView == null || (convertView.getId() != R.id.list_view_conflict_row) ) {
        view = (LinearLayout) mLayoutInflater.inflate(layoutId, parent, false);
      }
      TextView columnNameView = view.findViewById(R.id.list_item_column_display_name);
      columnNameView.setText(conflictColumn.getTitle());
      // the text view displaying the local value
      TextView localTextView = view.findViewById(R.id.list_item_local_text);
      localTextView.setText(conflictColumn.getLocalDisplayValue());
      TextView serverTextView = view.findViewById(R.id.list_item_server_text);
      serverTextView.setText(conflictColumn.getServerDisplayValue());
      // The decision the user has made. May be null if it hasn't been set.
      Resolution userDecision = mCallbacks.getConflictResolutionDecision(
          conflictColumn.getElementKey());

      RadioButton localButton = view.findViewById(R.id.list_item_local_radio_button);
      localButton.setText(mLocalButtonTextId);

      RadioButton serverButton = view.findViewById(R.id.list_item_server_radio_button);
      serverButton.setText(mServerButtonTextId);

      if (userDecision != null) {
        if (userDecision == Resolution.LOCAL) {
          localButton.setChecked(true);
          serverButton.setChecked(false);
        } else {
          // they've decided on the server version of the row.
          localButton.setChecked(false);
          serverButton.setChecked(true);
        }
      } else {
        localButton.setChecked(false);
        serverButton.setChecked(false);
      }
      // Alright. Now we need to set the click listeners. It's going to be a
      // little bit tricky. We want the list item as well to update the other
      // radiobutton as
      // appropriate. In order to do this, we're going to add the entire view
      // object, including itself, as the view's tag. That way we can get at
      // them to update appropriately.
      RelativeLayout localRow = view
          .findViewById(R.id.list_item_conflict_resolution_local_row);
      RelativeLayout serverRow = view
          .findViewById(R.id.list_item_conflict_resolution_server_row);
      localRow.setTag(view);
      serverRow.setTag(view);
      // We also need to add the position to each of the views, so that when
      // it's clicked we'll be able to figure out to which row it was
      // referring. We'll use the parent id for the key.
      localRow.setTag(R.id.list_view_conflict_row, position);
      serverRow.setTag(R.id.list_view_conflict_row, position);
      localRow.setOnClickListener(new ResolutionOnClickListener());
      localRow.setOnLongClickListener(new ResolutionOnLongClickListener());
      serverRow.setOnClickListener(new ResolutionOnClickListener());
      serverRow.setOnLongClickListener(new ResolutionOnLongClickListener());
      localRow.setEnabled(true);
      serverRow.setEnabled(true);
      return view;

    } else if (isConcordantColumnPosition(position)) {
      ConcordantColumn concordantColumn = this.mConcordantColumns.get(position);
      int layoutId = R.layout.list_item_concordant_row;
      LinearLayout view = (LinearLayout) convertView;
      if (convertView == null || (convertView.getId() != R.id.list_view_concordant_row) ) {
        view = (LinearLayout) mLayoutInflater.inflate(layoutId, parent, false);
      }
      // set the column name
      TextView columnNameView = view.findViewById(R.id.list_item_column_display_name);
      columnNameView.setText(concordantColumn.getTitle());
      // the text view displaying the local value
      TextView concordantTextView = view.findViewById(R.id.list_item_concordant_text);
      concordantTextView.setText(concordantColumn.getDisplayValue());
      return view;
    } else {
      WebLogger.getLogger(mAppName).e(TAG,
          "[getView] ran into trouble, position didn't match any of " + "the types!");
      return null;
    }
  }

  /**
   * The class that handles registering a user's choice and updating the view
   * appropriately. The view that adds this as a click listener must have
   * included the whole parent viewgroup as its tag.
   *
   * @author sudar.sam@gmail.com
   *
   */
  private class ResolutionOnClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {
      // First get the parent view of the whole conflict row, via which we'll
      // be able to get at the appropriate radio buttons.
      View conflictRowView = (View) v.getTag();
      int position = (Integer) v.getTag(R.id.list_view_conflict_row);
      ConflictColumn conflictColumn = mConflictColumns.get(position);
      RadioButton localButton = conflictRowView
          .findViewById(R.id.list_item_local_radio_button);
      RadioButton serverButton = conflictRowView
          .findViewById(R.id.list_item_server_radio_button);
      // Now we need to figure out if this is a server or a local click, which
      // we'll know by which view the click came in on.
      int viewId = v.getId();
      if (viewId == R.id.list_item_conflict_resolution_local_row) {
        // Then we have clicked on a local row.
        localButton.setChecked(true);
        serverButton.setChecked(false);
        setResolution(conflictColumn, Resolution.LOCAL);
      } else if (viewId == R.id.list_item_conflict_resolution_server_row) {
        // Then we've clicked on a server row.
        localButton.setChecked(false);
        serverButton.setChecked(true);
        setResolution(conflictColumn, Resolution.SERVER);
      } else {
        WebLogger.getLogger(mAppName)
            .e(TAG, "[onClick] wasn't a recognized id, not saving choice!");
      }
      v.clearFocus();
      mCallbacks.onDecisionMade();
    }

  }


  /**
   * The class that handles clearing a user's choice and updating the view
   * appropriately. The view that adds this as a click listener must have
   * included the whole parent viewgroup as its tag.
   *
   */
  private class ResolutionOnLongClickListener implements View.OnLongClickListener {

    @Override
    public boolean onLongClick(View v) {
      // First get the parent view of the whole conflict row, via which we'll
      // be able to get at the appropriate radio buttons.
      View conflictRowView = (View) v.getTag();
      int position = (Integer) v.getTag(R.id.list_view_conflict_row);
      ConflictColumn conflictColumn = mConflictColumns.get(position);
      RadioButton localButton = conflictRowView
          .findViewById(R.id.list_item_local_radio_button);
      RadioButton serverButton = conflictRowView
          .findViewById(R.id.list_item_server_radio_button);

      localButton.setChecked(false);
      serverButton.setChecked(false);
      setResolution(conflictColumn, null);
      v.clearFocus();
      mCallbacks.onDecisionMade();
      return true;
    }
  }
}
