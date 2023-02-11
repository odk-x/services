package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;

public class VerifyViewModel extends AbsSyncViewModel {

    private final MutableLiveData<VerifyServerSettingsActions> verifyActions;
    private final MutableLiveData<String> verifyType;

    public VerifyViewModel() {
        super();
        verifyActions = new MutableLiveData<>();
        verifyActions.setValue(VerifyServerSettingsActions.IDLE);

        verifyType = new MutableLiveData<>();
        verifyType.setValue("none");
    }

    public void updateVerifyAction(VerifyServerSettingsActions action) {
        verifyActions.setValue(action);
    }

    public VerifyServerSettingsActions getCurrentAction() {
        return verifyActions.getValue();
    }

    public void setVerifyType(String type){
        verifyType.setValue(type);
    }

    public String getVerifyType(){
        return verifyType.getValue();
    }
}
