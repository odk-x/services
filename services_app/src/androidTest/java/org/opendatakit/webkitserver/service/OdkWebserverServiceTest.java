package org.opendatakit.webkitserver.service;

import android.Manifest;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.IBinder;
import android.os.RemoteException;

import androidx.annotation.NonNull;
import androidx.test.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;
import androidx.test.rule.ServiceTestRule;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.TestConsts;
import org.opendatakit.consts.WebkitServerConsts;
import org.opendatakit.httpclientandroidlib.HttpStatus;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author mitchellsundt@gmail.com
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OdkWebserverServiceTest {

    private static final String TAG = "OdkWebserverServiceTest";


    private static final String HELLO_WORLD_HTML_TXT = "<HTML><BODY>Hello World!!!</BODY></HTML>";
    private static final String TEST_FILE_NAME = "Hello.html";
    private static final String TEST_DIR = "testfiles";

    @Rule
    public final ServiceTestRule mServiceRule = new ServiceTestRule();

    @Rule
    public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Rule
    public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

    @Before
    public void setUp() throws Exception {

        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        StaticStateManipulator.get().reset();
    }

    @After
    public void tearDown() {
        IWebkitServerInterface webkitServer = getIWebkitServerInterface();
        Context context = InstrumentationRegistry.getContext();
        context.unbindService(odkWebkitServiceConnection);
        // sleep to let this take effect
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            // ignore
            e.printStackTrace();
        }
    }

    private class ServiceConnectionWrapper implements ServiceConnection {

        @Override public void onServiceConnected(ComponentName name, IBinder service) {

            if (!name.getClassName().equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
                WebLogger.getLogger(TestConsts.APPNAME).e(TAG, "Unrecognized service");
                return;
            }
            synchronized (odkWebkitInterfaceBindComplete) {
                try {
                    webkitServerInterface = (service == null) ? null : IWebkitServerInterface.Stub.asInterface(service);
                } catch (IllegalArgumentException e) {
                    webkitServerInterface = null;
                }

                active = false;
                odkWebkitInterfaceBindComplete.notify();
            }
        }

        @Override public void onServiceDisconnected(ComponentName name) {
            synchronized (odkWebkitInterfaceBindComplete) {
                webkitServerInterface = null;
                active = false;
                odkWebkitInterfaceBindComplete.notify();
            }
        }
    }

    private final ServiceConnectionWrapper odkWebkitServiceConnection = new ServiceConnectionWrapper();
    private final Object odkWebkitInterfaceBindComplete = new Object();
    private IWebkitServerInterface webkitServerInterface;
    private boolean active = false;

    /**
     * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
     */
    private IWebkitServerInterface invokeBindService() throws InterruptedException {

        WebLogger.getLogger(TestConsts.APPNAME).i(TAG, "Attempting or polling on bind to Webkit Server "
            + "service");
        Intent bind_intent = new Intent();
        bind_intent.setClassName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
            WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);

        synchronized (odkWebkitInterfaceBindComplete) {
            if ( !active ) {
                active = true;
                Context context = InstrumentationRegistry.getContext();
                context.bindService(bind_intent, odkWebkitServiceConnection,
                    Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
                        Context.BIND_ADJUST_WITH_ACTIVITY :
                        0));
            }

            odkWebkitInterfaceBindComplete.wait();

            if (webkitServerInterface != null) {
                return webkitServerInterface;
            }
        }
        return null;
    }

    @NonNull
    private IWebkitServerInterface getIWebkitServerInterface() {

        // block waiting for it to be bound...
        for (;;) {
            try {

                synchronized (odkWebkitInterfaceBindComplete) {
                    if (webkitServerInterface != null) {
                        return webkitServerInterface;
                    }
                }

                // call method that waits on odkDbInterfaceBindComplete
                // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
                IWebkitServerInterface webkitServerInterface = invokeBindService();
                if ( webkitServerInterface != null ) {
                    return webkitServerInterface;
                }

            } catch (InterruptedException e) {
                // expected if we are waiting. Ignore because we log bind attempt if spinning.
            }
        }
    }

    @Test
    public void testBindingNRestart() {
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart");
        }
    }

    @Test
    public void testServingHelloWorldHtml() {
        File directoryLocation = new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), TEST_DIR);
        File fileLocation = new File(directoryLocation, TEST_FILE_NAME);

        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();

        PrintWriter writer = null;
        try {
            if(!directoryLocation.isDirectory()) {
                directoryLocation.mkdirs();
            }
            writer = new PrintWriter(fileLocation, "UTF-8");
            writer.println(HELLO_WORLD_HTML_TXT);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart");
        }

        HttpURLConnection connection = null;
        try {
            String urlStr = "http://" + WebkitServerConsts.HOSTNAME + ":" +
                Integer.toString(WebkitServerConsts.PORT) + "/" + TestConsts.APPNAME + "/" +
                ODKFileUtils.asUriFragment(TestConsts.APPNAME, fileLocation);

            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            if(connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(connection
                .getInputStream(), "UTF-8"));
            String responseStr = "";
            String segment = br.readLine();
            while (segment != null) {
                responseStr = responseStr + segment;
                segment = br.readLine();
            }
            br.close();
            assertTrue("RECEIVED:" + responseStr, HELLO_WORLD_HTML_TXT.equals(responseStr));
        } catch(IOException e) {
            e.printStackTrace();
            fail("GOT an IOException when trying to use the web server:" + e.getMessage());
        } catch(Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server:" + e.getMessage());
        } finally {
            if(connection != null){
                connection.disconnect();
            }
        }
    }

}
