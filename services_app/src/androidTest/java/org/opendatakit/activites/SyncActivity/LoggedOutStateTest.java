package org.opendatakit.activites.SyncActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isNotEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import android.content.Intent;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
import androidx.test.espresso.matcher.RootMatchers;
import androidx.test.espresso.matcher.ViewMatchers;

import org.junit.Before;
import org.junit.Test;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Map;

public class LoggedOutStateTest extends BaseSyncActivity {

    @Before
    public void setUp() {
        String APP_NAME = "testAppName";

        Intent intent = new Intent(getContext(), SyncActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        activityScenario = ActivityScenario.launch(intent);

        onView(withId(android.R.id.button2)).inRoot(RootMatchers.isDialog()).perform(ViewActions.click());
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(TEST_SERVER_URL);
            assertThat(serverProperties).isNotNull();
            props.setProperties(serverProperties);

            activity.updateViewModelWithProps();
        });
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.tvSignInWarnHeadingSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.btnSignInSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));

        onView(withId(R.id.tvSignInMethodSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvUsernameSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvLastSyncTimeSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_resolve_conflict)).check(doesNotExist());
        onView(withId(R.id.drawer_switch_sign_in_type)).check(doesNotExist());
        onView(withId(R.id.drawer_update_credentials)).check(doesNotExist());
    }

    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.inputSyncType)).check(matches(isNotEnabled()));
        onView(withId(R.id.autoInputSyncType)).check(matches(isNotEnabled()));
        onView(withId(R.id.btnStartSync)).check(matches(isNotEnabled()));
        onView(withId(R.id.tvSignInWarnHeadingSync)).check(matches(withText(getContext().getString(R.string.sign_in_to_sync))));
        onView(withId(R.id.btnDrawerLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_in_button_text))));
    }

    @Test
    public void verifyDrawerSignInButtonClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(LoginActivity.class.getName()));
        Intents.release();
    }
}
