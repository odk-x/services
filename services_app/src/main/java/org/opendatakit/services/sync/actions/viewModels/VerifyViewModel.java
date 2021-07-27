package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;

public class VerifyViewModel extends AbsSyncViewModel {

    public VerifyViewModel() {
        super();
        verifyActions=new MutableLiveData<>();
        verifyActions.setValue(VerifyServerSettingsActions.IDLE);
    }

    private MutableLiveData<VerifyServerSettingsActions> verifyActions;

    public void updateVerifyAction(VerifyServerSettingsActions action){
        if(verifyActions==null){
            verifyActions=new MutableLiveData<>();
        }
        verifyActions.setValue(action);
    }

    public VerifyServerSettingsActions getCurrentAction(){
        if(verifyActions==null){
            verifyActions=new MutableLiveData<>();
            verifyActions.setValue(VerifyServerSettingsActions.IDLE);
        }
        return verifyActions.getValue();
    }
}
