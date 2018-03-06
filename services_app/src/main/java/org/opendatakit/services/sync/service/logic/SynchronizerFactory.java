package org.opendatakit.services.sync.service.logic;

import org.opendatakit.services.sync.service.SyncExecutionContext;

public class SynchronizerFactory {
  public static Synchronizer create(SyncExecutionContext syncContext) {
    Synchronizer synchronizer;

    if (syncContext.getAggregateUri().startsWith("submit://")) {
      // TODO: make constant
      synchronizer = new AidlSynchronizer(syncContext.getApplication(), "org.opendatakit.submit", "org.opendatakit.submit.service.LocalSyncService");
    } else {
      synchronizer = new AggregateSynchronizer(syncContext);
    }

    return synchronizer;
  }
}
