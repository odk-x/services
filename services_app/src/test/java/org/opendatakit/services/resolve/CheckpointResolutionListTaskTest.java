package org.opendatakit.services.resolve;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import android.content.Context;
import android.os.Build;
import android.os.Environment;
import android.widget.ArrayAdapter;

import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.database.OdkConnectionFactoryInterface;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.task.CheckpointResolutionListTask;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;
import org.robolectric.shadows.ShadowEnvironment;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class CheckpointResolutionListTaskTest {

    private final String APP_NAME = "testAppName";
    private final String TABLE_ID = "11";
    private final String RESOLVING_ROWS = "Resolving row 1 of 0";
    private CheckpointResolutionListTask checkpointResolutionListTask;
    private ResolutionListener resolutionListener;
    private ArrayAdapter<ResolveRowEntry> adapter;
    private String mProgress;

    @Before
    public void setUp() {
        ShadowEnvironment.setExternalStorageState(Environment.MEDIA_MOUNTED);

        OdkConnectionFactoryInterface odkConnectionFactoryInterface =mock(OdkConnectionFactoryInterface.class);
        OdkConnectionFactorySingleton.set(odkConnectionFactoryInterface);
        adapter = new ArrayAdapter<>(getContext(), 1);

        checkpointResolutionListTask = new CheckpointResolutionListTask(getContext(), true, APP_NAME);
        checkpointResolutionListTask.setResolveRowEntryAdapter(adapter);
        checkpointResolutionListTask.execute();

        resolutionListener =mock(ResolutionListener.class);
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
        checkpointResolutionListTask.setResolveRowEntryAdapter(adapter);
        assertEquals(adapter.getCount(), checkpointResolutionListTask.getResolveRowEntryAdapter().getCount());
        mProgress = String.format(RESOLVING_ROWS, 1, adapter.getCount());
        assertEquals(mProgress, checkpointResolutionListTask.getProgress());

    }

    @After
    public void tearDown() throws Exception {
        checkpointResolutionListTask.cancel(false);

    }
    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }
}