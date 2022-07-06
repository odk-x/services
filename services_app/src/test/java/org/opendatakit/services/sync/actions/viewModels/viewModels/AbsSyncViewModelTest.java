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

import java.sql.Timestamp;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class AbsSyncViewModelTest {

    private AbsSyncViewModel absSyncViewModel;
    private final String APP_NAME = "testAppName";
    private final String USER_NAME = "testUser";
    private final String TEST_SERVER_URL = "https://testUrl.com";
    private final String LAST_SYNC_TIME = "2022-03-19 12:30:02";

    @Before
    public void setUp() throws Exception {
        absSyncViewModel = new AbsSyncViewModel();
    }

    @Test
    public void checkIfAppName_isAvailable() {
        absSyncViewModel.setAppName(APP_NAME);
        assertEquals(APP_NAME, absSyncViewModel.getAppName());
    }

    @Test
    public void checkIfAbsSyncViewModel_isStarted() {
        absSyncViewModel.setStarted(true);
        assertTrue(absSyncViewModel.getStarted());
    }

    @Test
    public void checkIfIsFirstLaunched_isAllowed() {
        absSyncViewModel.setIsFirstLaunch(true);
        assertEquals(true, absSyncViewModel.checkIsFirstLaunch().getValue());
    }

    @Test
    public void checkIfServerUrl_isAvailable() {
        absSyncViewModel.setServerUrl(TEST_SERVER_URL);
        assertEquals(TEST_SERVER_URL, absSyncViewModel.getServerUrl().getValue());
    }

    @Test
    public void checkIfServer_isVerified() {
        absSyncViewModel.setIsServerVerified(true);
        assertEquals(true, absSyncViewModel.checkIsServerVerified().getValue());
    }

    @Test
    public void checkIfAnonymousSignInUsed_isAllowed() {
        absSyncViewModel.setIsAnonymousSignInUsed(true);
        assertEquals(true, absSyncViewModel.checkIsAnonymousSignInUsed().getValue());
    }

    @Test
    public void checkIfAnonymous_isAllowed() {
        absSyncViewModel.setIsAnonymousAllowed(true);
        assertEquals(true, absSyncViewModel.checkIsAnonymousAllowed().getValue());
        assertTrue(absSyncViewModel.isAnonymousAllowed());

    }

    @Test
    public void checkIfCurrentUserState_isAvailable() {
        absSyncViewModel.setCurrentUserState(UserState.LOGGED_OUT);
        assertEquals(UserState.LOGGED_OUT, absSyncViewModel.getCurrentUserState().getValue());
        assertNotNull(absSyncViewModel.getUserState());
    }

    @Test
    public void checkIfUserName_isAvailable() {
        absSyncViewModel.setUsername(USER_NAME);
        assertEquals(USER_NAME, absSyncViewModel.getUsername().getValue());
    }

    @Test
    public void checkIfUser_isVerified() {
        absSyncViewModel.setIsUserVerified(true);
        assertEquals(true, absSyncViewModel.checkIsUserVerified().getValue());
    }

    @Test
    public void checkIfLastSyncTime_isVisible() {
        Timestamp time = Timestamp.valueOf(LAST_SYNC_TIME);
        absSyncViewModel.setLastSyncTime(time.getTime());
        assertEquals(String.valueOf(time.getTime()), absSyncViewModel.getLastSyncTime().getValue().toString());
    }

    @Test
    public void checkIFLastSyncTimeAvailable_isAllowed() {
        absSyncViewModel.setIsLastSyncTimeAvailable(true);
        assertEquals(true, absSyncViewModel.checkIsLastSyncTimeAvailable().getValue());
    }

    @Test
    public void checkIfSyncAttachmentState_isAvailable() {
        absSyncViewModel.updateSyncAttachmentState(SyncAttachmentState.REDUCED_SYNC);
        assertEquals(SyncAttachmentState.REDUCED_SYNC, absSyncViewModel.getCurrentSyncAttachmentState());
        assertNotNull(absSyncViewModel.getSyncAttachmentState().getValue());
    }
}