package org.opendatakit;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.replaceText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.RootMatchers.isDialog;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.android.gms.common.internal.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.isA;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.os.RemoteException;
import android.view.InputDevice;
import android.view.MotionEvent;
import android.view.View;
import android.widget.CheckBox;
import android.widget.Checkable;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.constraintlayout.widget.ConstraintLayout;
import androidx.preference.CheckBoxPreference;
import android.view.View;
import android.widget.Checkable;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.UiController;
import androidx.test.espresso.ViewAction;
import androidx.test.espresso.action.GeneralClickAction;
import androidx.test.espresso.action.Press;
import androidx.test.espresso.action.Tap;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.contrib.RecyclerViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.matcher.BoundedMatcher;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.Rule;
import org.opendatakit.services.R;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.matcher.BoundedMatcher;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;

public abstract class BaseUITest<T extends Activity> {
    protected final static String APP_NAME = "testAppName";
    protected final static String TEST_SERVER_URL = "https://testUrl.com";
    protected final static String TEST_PASSWORD = "testPassword";
    protected final static String TEST_USERNAME = "testUsername";
    protected final static String FONT_SIZE_XL = "Extra Large";
    protected final static String FONT_SIZE_L = "Large";
    protected final static String FONT_SIZE_M = "Medium";
    protected final static String FONT_SIZE_S = "Small";
    protected final static String FONT_SIZE_XS = "Extra Small";
    protected static final String SERVER_URL = "https://tables-demo.odk-x.org";
    protected ActivityScenario<T> activityScenario;

    @Rule
    public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Rule
    public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

    @Before
    public void setUp() {
        Intents.init();
        activityScenario = ActivityScenario.launch(getLaunchIntent());
        setUpPostLaunch();
    }

    protected abstract void setUpPostLaunch();
    protected abstract Intent getLaunchIntent();

    @After
    public void tearDown() throws Exception {
        if (activityScenario != null) activityScenario.close();
        Intents.release();
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    public void resetConfiguration() {
        PropertiesSingleton mProps = CommonToolProperties.get(getContext()
                , APP_NAME);
        mProps.clearSettings();
        LocalizationUtils.clearTranslations();
        File f = new File(ODKFileUtils.getTablesInitializationCompleteMarkerFile(APP_NAME));
        if (f.exists()) {
            f.delete();
        }
        ODKFileUtils.clearConfiguredToolFiles(APP_NAME);
    }

    public static ViewAction setChecked(final boolean checked) {
        return new ViewAction() {
            @Override
            public BaseMatcher<View> getConstraints() {
                return new BaseMatcher<View>() {
                    @Override
                    public boolean matches(Object item) {
                        return isA(Checkable.class).matches(item);
                    }

                    @Override
                    public void describeMismatch(Object item, Description mismatchDescription) {
                    }

                    @Override
                    public void describeTo(Description description) {
                    }
                };
            }

            @Override
            public String getDescription() {
                return "Checkbox checked value: " + checked;
            }

            @Override
            public void perform(UiController uiController, View view) {
                Checkable checkableView = (Checkable) view;
                if (checkableView.isChecked() != checked) {
                    click().perform(uiController, view);
                    checkableView.setChecked(checked);
                }
            }

        };
    }

    public static Matcher<View> atPosition(final int position, @NonNull final Matcher<View> itemMatcher) {
        checkNotNull(itemMatcher);
        return new BoundedMatcher<View, RecyclerView>(RecyclerView.class) {
            @Override
            public void describeTo(Description description) {
                description.appendText("has item at position " + position + ": ");
                itemMatcher.describeTo(description);
            }

            @Override
            protected boolean matchesSafely(final RecyclerView view) {
                RecyclerView.ViewHolder viewHolder = view.findViewHolderForAdapterPosition(position);
                if (viewHolder == null) {
                    // has no item on such position
                    return false;
                }
                return itemMatcher.matches(viewHolder.itemView);
            }
        };
    }
    public static ViewAction waitFor(long delay) {
        return new ViewAction() {
            @Override public Matcher<View> getConstraints() {
                return ViewMatchers.isRoot();
            }

            @Override public String getDescription() {
                return "wait for " + delay + "milliseconds";
            }

            @Override public void perform(UiController uiController, View view) {
                uiController.loopMainThreadForAtLeast(delay);
            }
        };
    }

    public  static void enableAdminMode() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.user_restrictions)),
                        click()));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(1,
                        click()));
        onView(withId(R.id.pwd_field)).perform(click());
        onView(withId(R.id.pwd_field)).perform(replaceText(TEST_PASSWORD));
        onView(withId(R.id.positive_button)).perform(ViewActions.click());
    }

    protected Activity getActivity() {
        final Activity[] activity1 = new Activity[1];
        activityScenario.onActivity(activity -> activity1[0] =activity);
        return activity1[0];
    }

}

