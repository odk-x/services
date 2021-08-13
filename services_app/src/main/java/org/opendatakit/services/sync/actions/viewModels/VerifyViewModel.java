package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;

public class VerifyViewModel extends AbsSyncViewModel {

    private final MutableLiveData<VerifyServerSettingsActions> verifyActions;

    public VerifyViewModel() {
        super();
        verifyActions = new MutableLiveData<>();
        verifyActions.setValue(VerifyServerSettingsActions.IDLE);
    }

    public void updateVerifyAction(VerifyServerSettingsActions action) {
        verifyActions.setValue(action);
    }

    public VerifyServerSettingsActions getCurrentAction() {
        return verifyActions.getValue();
    }
}
