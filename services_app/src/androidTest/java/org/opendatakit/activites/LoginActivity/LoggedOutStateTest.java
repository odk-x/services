package org.opendatakit.activites.LoginActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import static com.google.common.truth.Truth.assertThat;

import android.content.Intent;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;

import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Collections;
import java.util.Map;

public class LoggedOutStateTest extends BaseUITest<LoginActivity> {
    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(TEST_SERVER_URL);
            assertThat(serverProperties).isNotNull();
            props.setProperties(serverProperties);

            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "false"));

            activity.updateViewModelWithProps();
        });
    }
    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.tvServerUrlLogin)).check(matches(withText(TEST_SERVER_URL)));
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.drawer_resolve_conflict)).check(doesNotExist());
    }
    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), LoginActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }
}
