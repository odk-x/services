package org.opendatakit.services.sync.actions.activities;

import android.os.RemoteException;

import org.opendatakit.sync.service.IOdkSyncServiceInterface;

/**
 * @author mitchellsundt@gmail.com
 */
public interface DoSyncActionCallback {
  /**
   * Called with a null syncServiceInterface if the callback is being replaced
   * (cancelled) with another one. Some cancellations are lost (e.g., activity
   * is destroyed).
   *
   * @param syncServiceInterface
   * @throws RemoteException
    */
  void doAction(IOdkSyncServiceInterface syncServiceInterface) throws RemoteException;
}
