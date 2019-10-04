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

import android.os.RemoteException;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncStatus;

class IOdkSyncServiceInterfaceImpl extends IOdkSyncServiceInterface.Stub {

  private static final String LOGTAG = IOdkSyncServiceInterfaceImpl.class.getSimpleName();
  private final OdkSyncService syncService;

  public IOdkSyncServiceInterfaceImpl(OdkSyncService syncService) {
    this.syncService = syncService;
  }

  @Override
  public SyncStatus getSyncStatus(String appName) throws RemoteException {
    try {
      SyncStatus status = syncService.getStatus(appName);
      // WebLogger.getLogger(appName).d(LOGTAG,
      //     "SERVICE INTERFACE: getSyncStatus WITH appName:" + appName + " : " + status.name());
      return status;
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public SyncProgressEvent getSyncProgressEvent(String appName) throws RemoteException {
    try {
      // WebLogger.getLogger(appName).v(LOGTAG,
      // "SERVICE INTERFACE: getSyncProgress WITH appName:" + appName);
      return syncService.getSyncProgress(appName);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public SyncOverallResult getSyncResult(String appName) throws
      RemoteException {
    WebLogger.getLogger(appName).i(LOGTAG,
        "SERVICE INTERFACE: getSyncResult WITH appName:" + appName);
    try {
      return syncService.getSyncResult(appName);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public boolean resetServer(String appName, SyncAttachmentState attachmentState) throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG, "SERVICE INTERFACE: resetServer WITH appName:" + appName);
      return syncService.resetServer(appName, attachmentState);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public boolean synchronizeWithServer(String appName, SyncAttachmentState attachmentState) throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG,
          "SERVICE INTERFACE: synchronizeWithServer WITH appName:" + appName);
      return syncService.synchronizeWithServer(appName, attachmentState);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public boolean clearAppSynchronizer(String appName) throws RemoteException {
    try {
      return syncService.clearAppSynchronizer(appName);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public boolean verifyServerSettings(String appName) throws RemoteException {
    try {
      return syncService.verifyServerSettings(appName);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }
}
