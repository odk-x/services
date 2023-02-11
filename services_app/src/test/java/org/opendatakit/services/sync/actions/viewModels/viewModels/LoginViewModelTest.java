package org.opendatakit.services.sync.actions.viewModels.viewModels;

import static org.junit.Assert.assertEquals;

import android.os.Build;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.sync.actions.LoginActions;
import org.opendatakit.services.sync.actions.viewModels.LoginViewModel;
import org.opendatakit.services.utilities.Constants;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class LoginViewModelTest {

    private LoginViewModel loginViewModel;

    @Before
    public void setUp() throws Exception {
        loginViewModel = new LoginViewModel();
    }

    @Test
    public void checkIfLoginActions_isAvailable() {
        loginViewModel.updateLoginAction(LoginActions.MONITOR_VERIFYING);
        assertEquals(LoginActions.MONITOR_VERIFYING, loginViewModel.getCurrentAction());
    }

    @Test
    public void checkIfFunctionTypes_isAvailable() {
        loginViewModel.updateFunctionType(Constants.LOGIN_TYPE_SIGN_IN);
        assertEquals(Constants.LOGIN_TYPE_SIGN_IN, loginViewModel.getFunctionType().getValue());
    }
}