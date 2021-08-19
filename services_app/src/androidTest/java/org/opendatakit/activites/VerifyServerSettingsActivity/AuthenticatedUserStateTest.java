package org.opendatakit.activites.VerifyServerSettingsActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.isNotEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import android.content.Context;
import android.content.Intent;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
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
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.fragments.SetCredentialsFragment;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AuthenticatedUserStateTest {

    private final String TEST_SERVER_URL = "https://testUrl.com";

    private final String TEST_USERNAME = "testUsername";
    private final String TEST_PASSWORD = "testPassword";

    private ActivityScenario<VerifyServerSettingsActivity> activityScenario;

    @Before
    public void setUp() {
        String APP_NAME = "testAppName";

        Intent intent = new Intent(getContext(), VerifyServerSettingsActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        activityScenario = ActivityScenario.launch(intent);

        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            boolean isFirstLaunch = Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_FIRST_LAUNCH));
            assertThat(isFirstLaunch).isNotNull();

            if (isFirstLaunch) {
                props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "false"));
                activity.recreate();
            }

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
        onView(withId(R.id.tvUserHeadingVerifySettings)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvUsernameVerify)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.tvVerificationStatusVerify)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.tvLastSyncTimeVerify)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.btnStartVerifyUser)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_resolve_conflict)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_switch_sign_in_type)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_update_credentials)).check(matches(isDisplayed()));
    }

    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.tvUsernameVerify)).check(matches(withText(TEST_USERNAME)));
        onView(withId(R.id.tvVerificationStatusVerify)).check(matches(withText(getContext().getString(R.string.not_verified))));

        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_out_button_text))));
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
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(isDisplayed()));
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(isEnabled()));
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
        onView(withId(R.id.inputUsernameLogin)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextUsername)).check(matches(withText(TEST_USERNAME)));
        onView(withId(R.id.inputTextPassword)).check(matches(withText("")));
    }

    @Test
    public void verifyDrawerSignOutButtonClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).perform(ViewActions.click());

        onView(withId(R.id.tvUserHeadingVerifySettings)).check(matches(withText(getContext().getString(R.string.user_logged_out_label))));
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
            props.setProperties(serverProperties);
        });
        activityScenario.close();
    }

    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

}
