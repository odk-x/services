package org.opendatakit.utilities;

import android.view.View;
import android.view.ViewGroup;
import android.view.ViewParent;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class ViewMatchers {
    public static Matcher<View> childAtPosition(
            final Matcher<View> parentMatcher, final int position) {

        return new TypeSafeMatcher<View>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("Child at position " + position + " in parent ");
                parentMatcher.describeTo(description);
            }

            @Override
            public boolean matchesSafely(View view) {
                View currView = null;
                ViewParent parent = view.getParent();
                while (!parentMatcher.matches(parent)) {
                    currView = (View) parent;
                    parent = currView.getParent();
                }
                if (currView == null) {
                    throw new IllegalArgumentException("View is most likely null");
                }
                return parent instanceof ViewGroup && parentMatcher.matches(parent)
                        && currView.equals(((ViewGroup) parent).getChildAt(position));
            }
        };
    }
}
