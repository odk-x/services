/*
 * Copyright (C) 2017 University of Washington
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

package org.opendatakit.services.database.utilities;

import android.content.Context;
import android.net.Uri;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.FormsProviderAPI;
import org.opendatakit.provider.TablesProviderAPI;

/**
 * Wrap code to signal table and form providers that data may have changed.
 *
 * @author mitchellsundt@gmail.com
 */

public final class ProviderUtils {

  private ProviderUtils() {};

  public static final void notifyFormsProviderListener(Context context, String appName, String
      tableId) {

    // notify any listener of the FormsProvider that their result set is invalid
    try {
      Uri formsUri =
          Uri.withAppendedPath(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName),
              tableId);
      context.getContentResolver().notifyChange(formsUri, null);
    } catch (Exception e) {
      // swallow error if we can't notify of change...
      WebLogger.getLogger(appName).e("notifyFormsProviderListener", "notifyChange failed");
      WebLogger.getLogger(appName).printStackTrace(e);
    }

  }

  public static final void notifyTablesProviderListener(Context context, String appName, String
      tableId) {

    // notify any listener of the FormsProvider that their result set is invalid
    try {
      Uri formsUri =
          Uri.withAppendedPath(Uri.withAppendedPath(TablesProviderAPI.CONTENT_URI, appName),
              tableId);
      context.getContentResolver().notifyChange(formsUri, null);
    } catch (Exception e) {
      // swallow error if we can't notify of change...
      WebLogger.getLogger(appName).e("notifyTablesProviderListener", "notifyChange failed");
      WebLogger.getLogger(appName).printStackTrace(e);
    }

  }
}
