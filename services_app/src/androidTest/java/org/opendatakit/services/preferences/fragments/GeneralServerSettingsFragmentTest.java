package org.opendatakit.services.preferences.fragments;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.replaceText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.RootMatchers.isDialog;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isChecked;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.opendatakit.utilities.ViewMatchers.childAtPosition;

import android.content.Intent;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.contrib.RecyclerViewActions;

import junit.framework.AssertionFailedError;

import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class GeneralServerSettingsFragmentTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });

        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.server)),
                        click()));
    }

    @Ignore // OUTREACHY-BROKEN-TEST
    @Test
    public void checkBarcodeScanner_IsVisible() {
        onView(withId(R.id.action_barcode)).check(matches(isDisplayed()));
        onView(withId(R.id.action_barcode)).perform(click());
    }

    @Test
    public void whenServerUrlIsClicked_launchUrl() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(1,
                        click()));
        onView(withText(R.string.server_url))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));

        onView(withId(android.R.id.edit)).perform(replaceText(TEST_SERVER_URL));
        onView(allOf(withId(android.R.id.button1), withText("OK"))).perform(click());

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1),
                isDisplayed())).check(matches(withText(TEST_SERVER_URL)));

    }

    @Test
    public void whenServerSignOnCredentialIsChanged_checkIfAccessChangedToAnonymous() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(R.string.change_credential))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));
        
        onView(withText(R.string.anonymous)).check(matches(isDisplayed()));
        onView(allOf(withId(android.R.id.text1), withText(R.string.anonymous))).perform(click());

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(R.string.anonymous)));
    }

    @Test
    public void whenServerSignOnCredentialIsChanged_chekIfAccessChangedToUser() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(R.string.change_credential))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));
        onView(withText(R.string.username)).check(matches(isDisplayed()));
        onView(allOf(withId(android.R.id.text1), withText(R.string.username))).perform(click());

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(R.string.username)));
    }

    @Test
    public void whenUsernamePreferenceIsChanged_usernameChanged() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(3,
                        click()));
        onView(withText(R.string.change_username))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));

        onView(withId(android.R.id.edit)).perform(replaceText(TEST_USERNAME));
        onView(allOf(withId(android.R.id.button1), withText("OK"))).perform(click());

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 3),
                isDisplayed())).check(matches(withText(TEST_USERNAME)));
    }


    @Test
    public void whenServerPasswordIsChanged_enterNewPassword() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(4,
                        click()));
        onView(withId(R.id.pwd_field)).perform(click());
        onView(withId(R.id.pwd_field)).perform(replaceText(TEST_PASSWORD));
        onView(withId(R.id.positive_button)).perform(ViewActions.click());

    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }
}