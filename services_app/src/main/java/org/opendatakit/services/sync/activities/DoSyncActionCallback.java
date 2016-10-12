package org.opendatakit.services.sync.activities;

import android.os.RemoteException;
import org.opendatakit.sync.service.OdkSyncServiceInterface;

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
  void doAction(OdkSyncServiceInterface syncServiceInterface) throws RemoteException;
}
