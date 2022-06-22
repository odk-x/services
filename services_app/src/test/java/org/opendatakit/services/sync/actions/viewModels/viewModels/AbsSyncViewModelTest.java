package org.opendatakit.services.sync.actions.viewModels.viewModels;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import android.os.Build;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

import java.time.LocalTime;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class AbsSyncViewModelTest {

    private AbsSyncViewModel absSyncViewModel;

    @Before
    public void setUp() throws Exception {
        absSyncViewModel = new AbsSyncViewModel();
    }

    @Test
    public void checkAppName() throws Exception {
        String appName = "appName";
        absSyncViewModel.setAppName(appName);
        assertEquals(appName, absSyncViewModel.getAppName());
    }

    @Test
    public void checkIfStarted() {
        absSyncViewModel.setStarted(true);
        assertTrue(absSyncViewModel.getStarted());
    }

    @Test
    public void checkIfIsFirstLaunched() throws Exception {
        absSyncViewModel.setIsFirstLaunch(true);
        assertEquals(true, absSyncViewModel.checkIsFirstLaunch().getValue());
    }

    @Test
    public void checkServerUrl() throws Exception {
        String serverUrl = "https://odk-x.org/";
        absSyncViewModel.setServerUrl(serverUrl);
        assertEquals(serverUrl, absSyncViewModel.getServerUrl().getValue());
    }

    @Test
    public void checkIfIsServerVerified() {
        absSyncViewModel.setIsServerVerified(true);
        assertEquals(true, absSyncViewModel.checkIsServerVerified().getValue());
    }

    @Test
    public void checkIfIsAnonymousSignInUsed() {
        absSyncViewModel.setIsAnonymousSignInUsed(true);
        assertEquals(true, absSyncViewModel.checkIsAnonymousSignInUsed().getValue());
    }

    @Test
    public void checkIsAnonymousAllowed() {
        absSyncViewModel.setIsAnonymousAllowed(true);
        assertEquals(true, absSyncViewModel.checkIsAnonymousAllowed().getValue());
        assertTrue(absSyncViewModel.isAnonymousAllowed());

    }

    @Test
    public void checkCurrentUserState() {
        absSyncViewModel.setCurrentUserState(UserState.LOGGED_OUT);
        assertEquals(UserState.LOGGED_OUT, absSyncViewModel.getCurrentUserState().getValue());
        assertNotNull(absSyncViewModel.getUserState());
    }

    @Test
    public void checkUserName() {
        String userName = "john";
        absSyncViewModel.setUsername(userName);
        assertEquals(userName, absSyncViewModel.getUsername().getValue());
    }

    @Test
    public void checkIsUserVerified() {
        absSyncViewModel.setIsUserVerified(true);
        assertEquals(true, absSyncViewModel.checkIsUserVerified().getValue());
    }

    @Test
    public void checkLastSyncTime() {
        long time = LocalTime.parse("12:30").getHour();
        absSyncViewModel.setLastSyncTime(time);
        assertEquals(String.valueOf(time), absSyncViewModel.getLastSyncTime().getValue().toString());
    }

    @Test
    public void checkIsLastSyncTimeAvailable() {
        absSyncViewModel.setIsLastSyncTimeAvailable(true);
        assertEquals(true, absSyncViewModel.checkIsLastSyncTimeAvailable().getValue());
    }

    @Test
    public void checkSyncAttachmentState() {
        absSyncViewModel.updateSyncAttachmentState(SyncAttachmentState.REDUCED_SYNC);
        assertEquals(SyncAttachmentState.REDUCED_SYNC, absSyncViewModel.getCurrentSyncAttachmentState());
        absSyncViewModel.getSyncAttachmentState();
        assertNotNull(absSyncViewModel.getSyncAttachmentState().getValue());
    }
}