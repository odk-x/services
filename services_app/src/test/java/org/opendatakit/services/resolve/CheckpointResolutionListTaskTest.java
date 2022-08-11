package org.opendatakit.services.resolve;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import android.os.Build;
import android.os.Environment;
import android.widget.ArrayAdapter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.BaseTest;
import org.opendatakit.services.database.OdkConnectionFactoryInterface;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.task.CheckpointResolutionListTask;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;
import org.robolectric.shadows.ShadowEnvironment;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class CheckpointResolutionListTaskTest extends BaseTest {

    private CheckpointResolutionListTask checkpointResolutionListTask;

    @Before
    public void setUp() {
        ShadowEnvironment.setExternalStorageState(Environment.MEDIA_MOUNTED);

        OdkConnectionFactoryInterface odkConnectionFactoryInterface = mock(OdkConnectionFactoryInterface.class);
        resolutionListener = mock(ResolutionListener.class);
        OdkConnectionFactorySingleton.set(odkConnectionFactoryInterface);
        mAdapter = new ArrayAdapter<>(getContext(), 1);

        checkpointResolutionListTask = new CheckpointResolutionListTask(getContext(), true, APP_NAME);
        checkpointResolutionListTask.setResolveRowEntryAdapter(mAdapter);
        checkpointResolutionListTask.execute();
    }

    @Test
    public void checkIfAppName_isAvailable() {
        checkpointResolutionListTask.setAppName(APP_NAME);
        assertEquals(APP_NAME, checkpointResolutionListTask.getAppName());
    }

    @Test
    public void checkIfTableID_isVisible() {
        checkpointResolutionListTask.setTableId(TABLE_ID);
        assertEquals(TABLE_ID, checkpointResolutionListTask.getTableId());

    }

    @Test
    public void checkIfRowEntryAdapter_isResolved() {
        checkpointResolutionListTask.setResolveRowEntryAdapter(mAdapter);
        assertEquals(mAdapter.getCount(), checkpointResolutionListTask.getResolveRowEntryAdapter().getCount());
        mProgress = String.format(RESOLVING_ROWS, 1, mAdapter.getCount());
        assertEquals(mProgress, checkpointResolutionListTask.getProgress());

    }

    @After
    public void tearDown() throws Exception {
        checkpointResolutionListTask.cancel(false);
    }
}