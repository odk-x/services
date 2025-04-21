package org.opendatakit.logic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import android.Manifest;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Context;

import androidx.test.rule.GrantPermissionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opendatakit.services.sync.service.GlobalSyncNotificationManagerImpl;
import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;

public class GlobalSyncNotificationManagerImplTest {

    @Rule
    public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Rule
    public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

    @Mock
    private Service mockService;

    @Mock
    private NotificationManager mockNotificationManager;

    private GlobalSyncNotificationManagerImpl notificationManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockService.getSystemService(Context.NOTIFICATION_SERVICE)).thenReturn(mockNotificationManager);
        notificationManager = new GlobalSyncNotificationManagerImpl(mockService);
    }


    @Test(expected = NoAppNameSpecifiedException.class)
    public void testStartingSyncWithNullAppName() throws NoAppNameSpecifiedException {
        notificationManager.startingSync(null);
    }

    @Test
    public void testUpdateNotification() {
        notificationManager.updateNotification("testApp", "Syncing...", 100, 50, false);
        verify(mockNotificationManager).notify(eq("testApp"), anyInt(), any());
    }

    @Test
    public void testFinalErrorNotification() {
        notificationManager.finalErrorNotification("testApp", "Error occurred");
        verify(mockNotificationManager).notify(eq("testApp"), anyInt(), any());
    }

    @Test
    public void testClearNotification() {
        notificationManager.clearNotification("testApp", "Sync complete", "All data is synced");
        verify(mockNotificationManager).notify(eq("testApp"), anyInt(), any());
    }
}
