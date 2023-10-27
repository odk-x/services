package org.opendatakit.services.sync.actions.viewModels.viewModels;

import static org.junit.Assert.assertEquals;

import android.os.Build;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.sync.actions.SyncActions;
import org.opendatakit.services.sync.actions.viewModels.SyncViewModel;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class SyncViewModelTest {

    private SyncViewModel syncViewModel;

    @Before
    public void setUp() throws Exception {
        syncViewModel = new SyncViewModel();
    }

    @Test
    public void checkIfSyncAction_isAvailable() {
        syncViewModel.updateSyncAction(SyncActions.SYNC);
        assertEquals(SyncActions.SYNC, syncViewModel.getCurrentAction());
    }
}
