package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.LiveData;
import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.LoginActions;

public class LoginViewModel extends AbsSyncViewModel {

    private final MutableLiveData<LoginActions> loginActions;
    private final MutableLiveData<String> functionType;

    public LoginViewModel() {
        super();
        loginActions = new MutableLiveData<>();
        loginActions.setValue(LoginActions.IDLE);

        functionType = new MutableLiveData<>();
    }

    public void updateLoginAction(LoginActions action) {
        loginActions.setValue(action);
    }

    public void updateFunctionType(String type) {
        functionType.setValue(type);
    }

    public LoginActions getCurrentAction() {
        return loginActions.getValue();
    }

    public LiveData<String> getFunctionType() {
        return functionType;
    }
}
