package org.opendatakit.services.utilities;

import static android.os.Looper.getMainLooper;
import static org.junit.Assert.*;
import static org.robolectric.Shadows.shadowOf;

import android.os.Build;

import androidx.fragment.app.Fragment;
import androidx.test.core.app.ActivityScenario;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.androidlibrary.BuildConfig;
import org.opendatakit.services.resolve.checkpoint.CheckpointResolutionActivity;
import org.robolectric.Robolectric;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.android.controller.FragmentController;
import org.robolectric.annotation.Config;
import org.opendatakit.services.R;
import org.robolectric.annotation.Implements;
import org.robolectric.shadow.api.Shadow;
import org.robolectric.shadows.ShadowLooper;
import org.robolectric.util.FragmentTestUtil;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {Build.VERSION_CODES.O_MR1})
public class GoToAboutFragmentTest {
    private CheckpointResolutionActivity activity;

    @Before
    public void setUp() throws Exception {
        activity = Robolectric.buildActivity(CheckpointResolutionActivity.class)
                .create()
                .resume()
                .get();

    }

    @Test
    public void aboutFragmentLaunched() throws Exception {
        GoToAboutFragment.GotoAboutFragment(activity.getSupportFragmentManager(), R.id.checkpoint_resolver_activity_view);
        ShadowLooper.idleMainLooper();
        assertNotNull(activity.getSupportFragmentManager().findFragmentById(R.id.checkpoint_resolver_activity_view));
    }

}