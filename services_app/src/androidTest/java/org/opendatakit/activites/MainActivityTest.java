package org.opendatakit.activites;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import android.content.Context;

import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;
import androidx.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.utilities.StaticStateManipulator;

@RunWith(AndroidJUnit4.class)
public class MainActivityTest {

    private static final String APP_NAME = "MainActivityTest";

    @Rule
    public ActivityTestRule<MainActivity> mActivityTestRule = new ActivityTestRule<>(MainActivity.class);

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.action_sync)).check(doesNotExist());

        onView(withId(R.id.drawer_resolve_conflict)).check(doesNotExist());
        onView(withId(R.id.drawer_switch_sign_in_type)).check(doesNotExist());
        onView(withId(R.id.drawer_update_credentials)).check(doesNotExist());

        onView(withId(R.id.tvUsernameMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvLastSyncTimeMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.btnSignInMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
    }

    @Test
    public void verifyDefaultValues() {
        Context context = InstrumentationRegistry.getInstrumentation().getTargetContext();
        PropertiesSingleton props = CommonToolProperties.get(context, APP_NAME);

        String serverUrl = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);

        onView(withId(R.id.tvServerUrlMain))
                .check(matches(withText(serverUrl)));

        onView(withId(R.id.tvUserStateMain))
                .check(matches(withText(context.getString(R.string.logged_out))));
    }

}
