package org.opendatakit.logic;

import android.Manifest;
import android.content.Context;
import android.support.test.InstrumentationRegistry;
import android.support.test.rule.GrantPermissionRule;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.androidlibrary.R;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.desktop.WebLoggerDesktopFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mitchellsundt@gmail.com
 */
@RunWith(AndroidJUnit4.class)
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

        Context context = InstrumentationRegistry.getTargetContext();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);
        Map<String,String> properties = new HashMap<String,String>();

        // non-default value for font size
        properties.put(CommonToolProperties.KEY_FONT_SIZE, "29");
        // this is stored in SharedPreferences
        properties.put(CommonToolProperties.KEY_PASSWORD, "asdf");
        props.setProperties(properties);

        StaticStateManipulator.get().reset();

        props = CommonToolProperties.get(context, APPNAME);
        assertEquals(props.getProperty(CommonToolProperties.KEY_FONT_SIZE), "29");
        assertEquals(props.getProperty(CommonToolProperties.KEY_PASSWORD), "asdf");
    }


    /**
     * Setting or removing secure properties from a
     * non-privileged APK should fail.
     */
    @Test
    public void testSecureSetProperties() {

        Context context = InstrumentationRegistry.getTargetContext();

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

}
