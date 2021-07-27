package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.SyncActions;

public class SyncViewModel extends AbsSyncViewModel {

    public SyncViewModel() {
        super();
        syncAction=new MutableLiveData<>();
        syncAction.setValue(SyncActions.IDLE);
    }

    private MutableLiveData<SyncActions> syncAction;

    public void updateSyncAction(SyncActions syncActions){
        if(syncAction==null){
            syncAction=new MutableLiveData<>();
        }
        syncAction.setValue(syncActions);
    }

    public SyncActions getCurrentAction(){
        return syncAction.getValue();
    }
}
