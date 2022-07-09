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
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.services.database.OdkConnectionFactoryInterface;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.task.CheckpointResolutionListTask;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;
import org.robolectric.shadows.ShadowEnvironment;

import java.util.UUID;

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
    DbHandle dbHandleName;
    private  OdkConnectionFactorySingleton odkConnectionFactorySingleton;

    @Before
    public void setUp() {
        ShadowEnvironment.setExternalStorageState(Environment.MEDIA_MOUNTED);
        OdkConnectionFactoryInterface odkConnectionFactoryInterface = mock(OdkConnectionFactoryInterface.class);
        dbHandleName  = new DbHandle(UUID.randomUUID().toString());

        odkConnectionFactoryInterface.getConnection(APP_NAME,dbHandleName);
      //  odkConnectionFactorySingleton= mock(OdkConnectionFactorySingleton.class);
    /*    OdkConnectionFactoryInterface odkConnectionFactoryInterface = mock(OdkConnectionFactoryInterface.class);
        when(OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()).thenReturn(odkConnectionFactorySingleton);
        when(OdkConnectionFactorySingleton.set(odkConnectionFactoryInterface)).thenReturn(odkConnectionFactoryInterface);*/
        checkpointResolutionListTask = new CheckpointResolutionListTask(getContext(), true, APP_NAME);
        checkpointResolutionListTask.execute();
        resolutionListener = mock(ResolutionListener.class);
        adapter = new ArrayAdapter<>(getContext(), 1);
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

    @Test
    public void checkIfResolutionListener_isAvailable() {
        checkpointResolutionListTask.setResolutionListener(resolutionListener);
        assertNotNull(resolutionListener);
        checkpointResolutionListTask.clearResolutionListener(resolutionListener);
    }
    @After
    public void tearDown() throws Exception {
        System.out.println(checkpointResolutionListTask.getStatus());
        checkpointResolutionListTask.cancel(true);
    }
    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }
}