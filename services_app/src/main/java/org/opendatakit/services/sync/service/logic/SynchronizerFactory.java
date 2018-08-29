package org.opendatakit.services.sync.service.logic;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.sync.service.SyncExecutionContext;

public class SynchronizerFactory {
  public static Synchronizer create(SyncExecutionContext syncContext) {
    Synchronizer synchronizer;

    if (syncContext.getAggregateUri().startsWith(IntentConsts.SubmitLocalSync.URI_SCHEME)) {
      synchronizer = new AidlSynchronizer(
          syncContext,
          IntentConsts.SubmitLocalSync.PACKAGE_NAME,
          IntentConsts.SubmitLocalSync.SERVICE_CLASS_NAME
      );
    } else if (syncContext.getAggregateUri().startsWith(IntentConsts.SubmitPeerSync.URI_SCHEME)) {
      synchronizer = new AidlSynchronizer(
          syncContext,
          IntentConsts.SubmitPeerSync.PACKAGE_NAME,
          IntentConsts.SubmitPeerSync.SERVICE_CLASS_NAME
      );
    } else {
      synchronizer = new AggregateSynchronizer(syncContext);
    }

    return synchronizer;
  }
}
