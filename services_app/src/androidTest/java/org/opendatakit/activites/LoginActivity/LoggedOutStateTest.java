package org.opendatakit.activites.LoginActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;

import org.junit.Test;
import org.opendatakit.services.R;

public class LoggedOutStateTest extends BaseLoginActivity {
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
}
