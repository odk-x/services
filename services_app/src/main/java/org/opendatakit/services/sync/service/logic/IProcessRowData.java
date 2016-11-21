package org.opendatakit.services.sync.service.logic;

import org.opendatakit.services.sync.service.SyncExecutionContext;

/**
 * @author mitchellsundt@gmail.com
 */
public interface IProcessRowData {

  SyncExecutionContext getSyncExecutionContext();

  void publishUpdateNotification(int idResource, String tableId);

  void publishUpdateNotification(int idResource, String tableId, double percentage);

}
