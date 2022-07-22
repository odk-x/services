package org.opendatakit;

import static com.google.android.gms.common.internal.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.isA;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.view.View;
import android.widget.Checkable;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.UiController;
import androidx.test.espresso.ViewAction;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.matcher.BoundedMatcher;
import androidx.test.platform.app.InstrumentationRegistry;

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
    protected static final String SERVER_URL = "https://tables-demo.odk-x.org";
    protected ActivityScenario<T> activityScenario;

    @Before
    public void setUp() {
        activityScenario = ActivityScenario.launch(getLaunchIntent());
        setUpPostLaunch();
        Intents.init();
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
}

