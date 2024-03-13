package org.opendatakit.services.sync.actions.viewModels.viewModels;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import android.os.Build;

import androidx.lifecycle.Observer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.BaseTest;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class AbsSyncViewModelTest extends BaseTest {

    private AbsSyncViewModel absSyncViewModel;

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

    @Test
    public void testNullValues() {
        absSyncViewModel.setAppName(null);
        assertNull(absSyncViewModel.getAppName());

        absSyncViewModel.setServerUrl(null);
        assertNull(absSyncViewModel.getServerUrl().getValue());

        absSyncViewModel.setUsername(null);
        assertNull(absSyncViewModel.getUsername().getValue());
    }

    @Test
    public void testIsFirstLaunchFlags() {
        absSyncViewModel.setIsFirstLaunch(true);
        assertTrue(absSyncViewModel.checkIsFirstLaunch().getValue());

        absSyncViewModel.setIsFirstLaunch(false);
        assertFalse(absSyncViewModel.checkIsFirstLaunch().getValue());
    }

    @Test
    public void testServerUrlExtremeValue() {
        String longUrl = "http://" + String.join("", Collections.nCopies(100000, "a")) + ".com";
        absSyncViewModel.setServerUrl(longUrl);
        assertEquals(longUrl, absSyncViewModel.getServerUrl().getValue());
    }

    @Test
    public void testUserState() {
        for (UserState state : UserState.values()) {
            absSyncViewModel.setCurrentUserState(state);
        }
        assertEquals(UserState.values()[UserState.values().length - 1], absSyncViewModel.getCurrentUserState().getValue());
    }

    @Test
    public void testConcurrentAccessToLastSyncTime() throws InterruptedException {
        final int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicBoolean allThreadsSuccessful = new AtomicBoolean(true);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    Long currentLastSyncTime = absSyncViewModel.getLastSyncTime().getValue();
                } catch (Exception e) {
                    allThreadsSuccessful.set(false);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        assertTrue("Some threads encountered errors while accessing last sync time", allThreadsSuccessful.get());
    }


    @Test
    public void testLastSyncTimeValueConsistencyAcrossThreads() throws InterruptedException {
        final int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicLong initialLastSyncTime = new AtomicLong();

        Long initialValue = absSyncViewModel.getLastSyncTime().getValue();
        initialLastSyncTime.set(initialValue != null ? initialValue : 0);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    Long currentLastSyncTime = absSyncViewModel.getLastSyncTime().getValue();
                    if (currentLastSyncTime != null && currentLastSyncTime != initialLastSyncTime.get()) {
                        initialLastSyncTime.set(-1);
                    }
                } catch (Exception e) {
                    initialLastSyncTime.set(-1);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        assertEquals("Last sync time is consistent across threads", 0, initialLastSyncTime.get());
    }

}