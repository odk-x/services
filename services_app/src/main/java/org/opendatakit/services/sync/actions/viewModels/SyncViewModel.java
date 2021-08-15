package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.SyncActions;

public class SyncViewModel extends AbsSyncViewModel {

    private final MutableLiveData<SyncActions> syncAction;

    public SyncViewModel() {
        super();
        syncAction = new MutableLiveData<>();
        syncAction.setValue(SyncActions.IDLE);
    }

    public void updateSyncAction(SyncActions syncActions) {
        syncAction.setValue(syncActions);
    }

    public SyncActions getCurrentAction() {
        return syncAction.getValue();
    }
}
