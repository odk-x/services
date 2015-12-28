/*
 * Copyright (C) 2015 University of Washington
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

package org.opendatakit;

public class IntentConsts {
  
  public static final String INTENT_KEY_APP_NAME = "appName";
  public static final String INTENT_KEY_TABLE_ID = "tableId";
  public static final String INTENT_KEY_INSTANCE_ID = "instanceId";
  // for rowpath and/or configpath
  public static final String INTENT_KEY_URI_FRAGMENT = "uriFragment";
  public static final String INTENT_KEY_CONTENT_TYPE = "contentType";

  /**
   * Intent Extras:
   * <ol><li>INTENT_KEY_APP_NAME</li>
   * <li>INTENT_KEY_TABLE_ID</li>
   * <li>INTENT_KEY_INSTANCE_ID (optional)</li></ol>
   */
  public class ResolveCheckpoint {
    public static final String APPLICATION_NAME = "org.opendatakit.core";
    public static final String ACTIVITY_NAME =
        "org.opendatakit.resolve.checkpoint.CheckpointResolutionActivity";
  }

  /**
   * Intent Extras:
   * <ol><li>INTENT_KEY_APP_NAME</li>
   * <li>INTENT_KEY_TABLE_ID</li>
   * <li>INTENT_KEY_INSTANCE_ID (optional)</li></ol>
   */
  public class ResolveConflict {
    public static final String APPLICATION_NAME = "org.opendatakit.core";
    public static final String ACTIVITY_NAME =
        "org.opendatakit.resolve.conflict.ConflictResolutionActivity";
  }

  /**
   * Intent Extras:
   * <ol><li>INTENT_KEY_APP_NAME</li></ol>
   */
  public class Sync {
    public static final String APPLICATION_NAME = "org.opendatakit.core";
    public static final String ACTIVITY_NAME =
        "org.opendatakit.sync.activities.SyncActivity";
  }
}
