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
package org.opendatakit.services.sync.service;

import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;

public interface GlobalSyncNotificationManager {

  // Time Unit: milliseconds.
  // 5 minutes. Amount of time to hold onto the details of a sync outcome.
  // after this amount of time, if there are no outstanding sync actions
  // and if there are no active bindings, then the sync service will shut
  // down.
  long RETENTION_PERIOD = 300000L;

  int SYNC_NOTIFICATION_ID = 0;

  void startingSync(String appName) throws NoAppNameSpecifiedException;

  void stoppingSync(String appName) throws NoAppNameSpecifiedException;

  void updateNotification( String appName, String text,
                                              int maxProgress, int progress, boolean
                                                   indeterminateProgress);

  void finalErrorNotification(String appName, String text);

  void finalConflictNotification(String appName, String text);

  void clearNotification(String appName, String title, String text);

  void clearVerificationNotification(String appName, String title, String text);
}
