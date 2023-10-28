package org.opendatakit.logic;

import android.Manifest;
import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.desktop.WebLoggerDesktopFactoryImpl;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mitchellsundt@gmail.com
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PropertiesTest {

    private static final String APPNAME = "unittestProp";

    @Rule
    public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Rule
    public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

    @Before
    public void setUp() throws Exception {
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(APPNAME);

        StaticStateManipulator.get().reset();
        WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
    }

    @Test
    public void testSimpleProperties() {

        Context context = InstrumentationRegistry.getInstrumentation().getTargetContext();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);
        Map<String,String> properties = new HashMap<String,String>();

        // non-default value for font size
        properties.put(CommonToolProperties.KEY_FONT_SIZE, "29");
        // this is stored in SharedPreferences
        properties.put(CommonToolProperties.KEY_PASSWORD, "asdf");
        properties.put(CommonToolProperties.KEY_USERNAME, "demo_user");
        props.setProperties(properties);

        StaticStateManipulator.get().reset();

        props = CommonToolProperties.get(context, APPNAME);
        assertEquals(props.getProperty(CommonToolProperties.KEY_FONT_SIZE), "29");
        assertEquals(props.getProperty(CommonToolProperties.KEY_PASSWORD), "asdf");
        assertEquals(props.getProperty(CommonToolProperties.KEY_USERNAME), "demo_user");
    }


    /**
     * Setting or removing secure properties from a
     * non-privileged APK should fail.
     */
    @Test
    public void testSecureSetProperties() {

        Context context = InstrumentationRegistry.getInstrumentation().getTargetContext();

        StaticStateManipulator.get().reset();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);
        String[] secureKeys = {
            CommonToolProperties.KEY_PASSWORD,
            CommonToolProperties.KEY_AUTHENTICATED_USER_ID,
            CommonToolProperties.KEY_ROLES_LIST,
            CommonToolProperties.KEY_DEFAULT_GROUP,
            CommonToolProperties.KEY_USERS_LIST,
            CommonToolProperties.KEY_ADMIN_PW
        };

        for ( String secureKey : secureKeys ) {
            // this is stored in SharedPreferences
            props.setProperties(Collections.singletonMap(secureKey, "asdf" + secureKey.hashCode()));
            assertEquals(props.getProperty(secureKey), "asdf" + secureKey.hashCode());

            // and verify remove works
            props.setProperties(Collections.singletonMap(secureKey, (String) null));

            assertNull("remove: " + secureKey, props.getProperty(secureKey));

        }
    }

    @Test
    public void testFontSizeEdgeCases() {

        // Constant Minimum and Maximum font sizes represented as MIN_FONT_SIZE AND MAX_FONT_SIZE
        final int MIN_FONT_SIZE = 8;
        final int MAX_FONT_SIZE = 74;

        Context context = InstrumentationRegistry.getInstrumentation().getTargetContext();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);

        // Set the font size to the smallest value
        props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FONT_SIZE, String.valueOf(MIN_FONT_SIZE)));

        StaticStateManipulator.get().reset();

        int minFontSize = Integer.parseInt(props.getProperty(CommonToolProperties.KEY_FONT_SIZE));
        assertEquals(minFontSize, MIN_FONT_SIZE);

        // Set the font size to the largest value
        props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FONT_SIZE, String.valueOf(MAX_FONT_SIZE)));

        StaticStateManipulator.get().reset();

        int maxFontSize = Integer.parseInt(props.getProperty(CommonToolProperties.KEY_FONT_SIZE));
        assertEquals(maxFontSize, MAX_FONT_SIZE);
    }
}
