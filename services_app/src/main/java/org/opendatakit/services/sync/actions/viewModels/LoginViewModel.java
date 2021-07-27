package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.MutableLiveData;

import org.opendatakit.services.sync.actions.LoginActions;

public class LoginViewModel extends AbsSyncViewModel {

    public LoginViewModel() {
        super();
        loginActions=new MutableLiveData<>();
        loginActions.setValue(LoginActions.IDLE);
    }

    private MutableLiveData<LoginActions> loginActions;

    public void updateLoginAction(LoginActions action){
        if(loginActions==null){
            loginActions=new MutableLiveData<>();
        }
        loginActions.setValue(action);
    }

    public LoginActions getCurrentAction(){
        if(loginActions==null){
            loginActions=new MutableLiveData<>();
            loginActions.setValue(LoginActions.IDLE);
        }
        return loginActions.getValue();
    }

}
