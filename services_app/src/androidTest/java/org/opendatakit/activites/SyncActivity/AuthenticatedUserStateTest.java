package org.opendatakit.activites.SyncActivity;

import static androidx.test.espresso.Espresso.onData;
import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.isNotEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
import androidx.test.espresso.matcher.RootMatchers;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.fragments.SetCredentialsFragment;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;
import org.opendatakit.services.utilities.DateTimeUtil;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class AuthenticatedUserStateTest {

    private final String TEST_SERVER_URL = "https://testUrl.com";

    private final String TEST_USERNAME = "testUsername";
    private final String TEST_PASSWORD = "testPassword";

    private ActivityScenario<SyncActivity> activityScenario;

    @Before
    public void setUp() {
        String APP_NAME = "testAppName";

        Intent intent = new Intent(getContext(), SyncActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        activityScenario = ActivityScenario.launch(intent);

        onView(withId(android.R.id.button2)).perform(ViewActions.click());
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(TEST_SERVER_URL);
            assertThat(serverProperties).isNotNull();
            props.setProperties(serverProperties);

            Map<String, String> userProperties = SetCredentialsFragment.getCredentialsProperty(TEST_USERNAME, TEST_PASSWORD);
            assertThat(userProperties).isNotNull();
            props.setProperties(userProperties);

            activity.updateViewModelWithProps();
        });
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.tvSignInWarnHeadingSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.btnSignInSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));

        onView(withId(R.id.tvSignInMethodSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.tvUsernameSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.tvLastSyncTimeSync)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_resolve_conflict)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_switch_sign_in_type)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_update_credentials)).check(matches(isDisplayed()));
    }

    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.inputSyncType)).check(matches(isEnabled()));
        onView(withId(R.id.autoInputSyncType)).check(matches(isEnabled()));
        onView(withId(R.id.btnStartSync)).check(matches(isEnabled()));

        onView(withId(R.id.tvSignInMethodSync)).check(matches(withText(getContext().getString(R.string.authenticated_user))));
        onView(withId(R.id.tvUsernameSync)).check(matches(withText(TEST_USERNAME)));
        onView(withId(R.id.tvLastSyncTimeSync)).check(matches(withText(getContext().getString(R.string.last_sync_not_available))));

        onView(withId(R.id.btnDrawerLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_out_button_text))));
    }

    @Test
    public void verifyChangeSyncTypeTest() {
        String [] syncTypes = getContext().getResources().getStringArray(R.array.sync_attachment_option_names);
        String type=syncTypes[new Random().nextInt(4)];

        onView(withId(R.id.autoInputSyncType)).perform(ViewActions.click());
        onData(allOf(is(instanceOf(String.class)), is(type)))
                .inRoot(RootMatchers.withDecorView(not(is(getActivity().getWindow().getDecorView()))))
                .perform(ViewActions.click());

        activityScenario.recreate();
        onView(withId(R.id.autoInputSyncType)).check(matches(withText(type)));
    }

    @Test
    public void verifyLastSyncTimeTest() {
        onView(withId(R.id.tvLastSyncTimeSync)).check(matches(withText(getContext().getString(R.string.last_sync_not_available))));
        long currentTime = new Date().getTime();
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_LAST_SYNC_INFO, Long.toString(currentTime)));
            activity.updateViewModelWithProps();
        });
        onView(withId(R.id.tvLastSyncTimeSync)).check(matches(withText(DateTimeUtil.getDisplayDate(currentTime))));
    }

    @Test
    public void verifyDrawerResolveConflictsClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_resolve_conflict)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AllConflictsResolutionActivity.class.getName()));
        Intents.release();
    }

    @Test
    public void verifyDrawerSwitchSignInTypeClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_switch_sign_in_type)).perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(LoginActivity.class.getName()));
        Intents.release();

        onView(withId(R.id.tvTitleLogin)).check(matches(withText(getContext().getString(R.string.switch_sign_in_type))));
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(withText(R.string.anonymous_user)));
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(isEnabled()));
        onView(withId(R.id.btnUserSignInLogin)).check(matches(withText(R.string.drawer_item_update_credentials)));
        onView(withId(R.id.btnUserSignInLogin)).check(matches(isNotEnabled()));
    }

    @Test
    public void verifySwitchSignInWhenAnonymousNotAllowed() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());

            Map<String, String> properties = new HashMap<>();
            properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(true));
            properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, Boolean.toString(false));
            props.setProperties(properties);

            activity.updateViewModelWithProps();
        });

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_switch_sign_in_type)).check(matches(isNotEnabled()));
    }

    @Test
    public void verifyDrawerUpdateCredentialsClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_update_credentials)).perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(LoginActivity.class.getName()));
        Intents.release();

        onView(withId(R.id.tvTitleLogin)).check(matches(withText(getContext().getString(R.string.drawer_item_update_credentials))));
        onView(withId(R.id.btnAuthenticateUserLogin)).check(matches(withText(R.string.drawer_item_update_credentials)));
        onView(withId(R.id.inputUsernameLogin)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextUsername)).check(matches(withText(TEST_USERNAME)));
        onView(withId(R.id.inputTextPassword)).check(matches(withText("")));
    }

    @Test
    public void verifyDrawerSignOutButtonClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).perform(ViewActions.click());

        onView(withId(R.id.tvSignInWarnHeadingSync)).check(matches(isDisplayed()));
        onView(withId(R.id.btnDrawerLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_in_button_text))));
    }

    @After
    public void clearTestEnvironment() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(
                    activity.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url)
            );
            assertThat(serverProperties).isNotNull();
            serverProperties.put(CommonToolProperties.KEY_FIRST_LAUNCH,"true");
            serverProperties.put(CommonToolProperties.KEY_SYNC_ATTACHMENT_STATE, null);
            props.setProperties(serverProperties);
        });
        activityScenario.close();
    }

    private Activity getActivity() {
        final Activity[] activity1 = new Activity[1];
        activityScenario.onActivity(activity -> activity1[0] =activity);
        return activity1[0];
    }

    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

}