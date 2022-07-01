package org.opendatakit.activites.MainActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
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
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.fragments.ChooseSignInTypeFragment;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;
import org.opendatakit.services.utilities.DateTimeUtil;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class AnonymousStateTest extends BaseMainActivity {

    @Before
    public void setUp() {
        String APP_NAME = "testAppName";

        Intent intent = new Intent(getContext(), MainActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        activityScenario = ActivityScenario.launch(intent);

        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(
                    activity.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url)
            );
            assertThat(serverProperties).isNotNull();
            props.setProperties(serverProperties);

            Map<String, String> anonymousProperties = ChooseSignInTypeFragment.getAnonymousProperties();
            assertThat(anonymousProperties).isNotNull();
            props.setProperties(anonymousProperties);

            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "false"));

            activity.updateViewModelWithProps();
        });
        Intents.init();
    }
    @Test
    public void checkFirstStartupTest() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "true"));
            activity.recreate();
        });

        onView(withId(android.R.id.button1)).inRoot(RootMatchers.isDialog()).perform(ViewActions.click());

        onView(withId(R.id.inputServerUrl)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextServerUrl)).check(matches(withText(SERVER_URL)));
    }
    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.action_sync)).check(matches(isDisplayed()));

        onView(withId(R.id.tvUsernameMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvLastSyncTimeMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.btnSignInMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());

        onView(withId(R.id.drawer_resolve_conflict)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_switch_sign_in_type)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_update_credentials)).check(doesNotExist());
    }

    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.tvUserStateMain))
                .check(matches(withText(getContext().getString(R.string.anonymous_user))));

        onView(withId(R.id.tvLastSyncTimeMain))
                .check(matches(withText(getContext().getString(R.string.last_sync_not_available))));

        onView(withId(R.id.btnDrawerLogin))
                .check(matches(withText(getContext().getString(R.string.drawer_sign_out_button_text))));
    }

    @Test
    public void verifyLastSyncTimeTest() {
        onView(withId(R.id.tvLastSyncTimeMain)).check(matches(withText(getContext().getString(R.string.last_sync_not_available))));
        long currentTime = new Date().getTime();
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_LAST_SYNC_INFO, Long.toString(currentTime)));
            activity.updateViewModelWithProps();
        });
        onView(withId(R.id.tvLastSyncTimeMain)).check(matches(withText(DateTimeUtil.getDisplayDate(currentTime))));
    }

    @Test
    public void verifyToolbarSyncItemClick() {
        onView(withId(R.id.action_sync)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(SyncActivity.class.getName()));
    }

    @Test
    public void verifyDrawerResolveConflictsClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_resolve_conflict)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AllConflictsResolutionActivity.class.getName()));
    }

    @Test
    public void verifyDrawerSwitchSignInTypeClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_switch_sign_in_type)).perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(LoginActivity.class.getName()));

        onView(withId(R.id.tvTitleLogin)).check(matches(withText(getContext().getString(R.string.switch_sign_in_type))));
        onView(withId(R.id.btnAuthenticateUserLogin)).check(matches(withText(getContext().getString(R.string.sign_in_using_credentials))));
        onView(withId(R.id.inputUsernameLogin)).check(matches(isDisplayed()));
    }

    @Test
    public void verifyDrawerSignOutButtonClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).perform(ViewActions.click());

        onView(withId(R.id.tvUserStateMain)).check(matches(withText(getContext().getString(R.string.logged_out))));
        onView(withId(R.id.btnDrawerLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_in_button_text))));

        onView(withId(R.id.btnSignInMain)).check(matches(isDisplayed()));
    }
}
