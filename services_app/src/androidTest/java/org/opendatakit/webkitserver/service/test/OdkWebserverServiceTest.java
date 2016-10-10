package org.opendatakit.webkitserver.service.test;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;

import org.opendatakit.TestConsts;
import org.opendatakit.consts.WebkitServerConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.httpclientandroidlib.HttpStatus;
import org.opendatakit.webkitserver.service.WebkitServerInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import android.test.ApplicationTestCase;
import org.opendatakit.services.application.Services;
import org.opendatakit.utilities.StaticStateManipulator;

/**
 * @author mitchellsundt@gmail.com
 */
public class OdkWebserverServiceTest extends ApplicationTestCase<Services> {

    private static final String TAG = "OdkWebserverServiceTest";


    private static final String HELLO_WORLD_HTML_TXT = "<HTML><BODY>Hello World!!!</BODY></HTML>";
    private static final String TEST_FILE_NAME = "Hello.html";
    private static final String TEST_DIR = "testfiles";

    public OdkWebserverServiceTest(String name) {
        super(Services.class);
        setName(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        createApplication();

        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        StaticStateManipulator.get().reset();
    }

    @Override
    protected void tearDown() {
        WebkitServerInterface webkitServer = getWebkitServerInterface();
        getContext().unbindService(odkWebkitServiceConnection);
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
                    webkitServerInterface = (service == null) ? null : WebkitServerInterface.Stub.asInterface(service);
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
    private WebkitServerInterface webkitServerInterface;
    private boolean active = false;

    /**
     * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
     */
    private WebkitServerInterface invokeBindService() throws InterruptedException {

        WebLogger.getLogger(TestConsts.APPNAME).i(TAG, "Attempting or polling on bind to Webkit Server "
            + "service");
        Intent bind_intent = new Intent();
        bind_intent.setClassName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
            WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);

        synchronized (odkWebkitInterfaceBindComplete) {
            if ( !active ) {
                active = true;
                getApplication().bindService(bind_intent, odkWebkitServiceConnection,
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
    private WebkitServerInterface getWebkitServerInterface() {

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
                WebkitServerInterface webkitServerInterface = invokeBindService();
                if ( webkitServerInterface != null ) {
                    return webkitServerInterface;
                }

            } catch (InterruptedException e) {
                // expected if we are waiting. Ignore because we log bind attempt if spinning.
            }
        }
    }

    public void testBindingNRestart() {
        WebkitServerInterface serviceInterface = getWebkitServerInterface();
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testServingHelloWorldHtml() {
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        File directoryLocation = new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), TEST_DIR);
        File fileLocation = new File(directoryLocation, TEST_FILE_NAME);

        WebkitServerInterface serviceInterface = getWebkitServerInterface();

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
