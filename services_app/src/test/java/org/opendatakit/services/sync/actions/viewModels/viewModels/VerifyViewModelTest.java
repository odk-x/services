package org.opendatakit.services.sync.actions.viewModels.viewModels;

import static org.junit.Assert.assertEquals;

import android.os.Build;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.BaseTest;
import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;
import org.opendatakit.services.sync.actions.viewModels.VerifyViewModel;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class VerifyViewModelTest extends BaseTest {

    private VerifyViewModel verifyViewModel;

    @Before
    public void setUp() throws Exception {
        verifyViewModel = new VerifyViewModel();
    }

    @Test
    public void checkIfVerifyActions_isAvailable() {
        verifyViewModel.updateVerifyAction(VerifyServerSettingsActions.VERIFY);
        assertEquals(VerifyServerSettingsActions.VERIFY, verifyViewModel.getCurrentAction());
    }

    @Test
    public void checkVerifyType_isVisible() {
        verifyViewModel.setVerifyType(TEST_VERIFY_TYPE);
        assertEquals(TEST_VERIFY_TYPE, verifyViewModel.getVerifyType());
    }
}
