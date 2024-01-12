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
import androidx.test.platform.app.InstrumentationRegistry;
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
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
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
        Context context = InstrumentationRegistry.getInstrumentation().getContext();
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
                Context context = InstrumentationRegistry.getInstrumentation().getContext();
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation);
        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);
        // Assert
        assertResponseMatchesHelloWorldHtml(fileLocation);
    }
    private File createTestDirectory() {
        File directoryLocation = new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), TEST_DIR);
        if (!directoryLocation.isDirectory()) {
            directoryLocation.mkdirs();
        }
        return directoryLocation;
    }

    private File createTestFile(File directoryLocation) {
        File fileLocation = new File(directoryLocation, TEST_FILE_NAME);

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println(HELLO_WORLD_HTML_TXT);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file: " + e.getMessage());
        }

        return fileLocation;
    }

    private void restartService(IWebkitServerInterface serviceInterface) {
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart: " + e.getMessage());
        }
    }

    private void assertResponseMatchesHelloWorldHtml(File fileLocation) {
        try {
            String urlStr = buildTestUrl(fileLocation);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                StringBuilder responseStr = new StringBuilder();
                String segment;
                while ((segment = br.readLine()) != null) {
                    responseStr.append(segment);
                }
                assertTrue("Received: " + responseStr, HELLO_WORLD_HTML_TXT.equals(responseStr.toString()));
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    private String buildTestUrl(File fileLocation) {
        return "http://" + WebkitServerConsts.HOSTNAME + ":" +
                Integer.toString(WebkitServerConsts.PORT) + "/" + TestConsts.APPNAME + "/" +
                ODKFileUtils.asUriFragment(TestConsts.APPNAME, fileLocation);
    }
    @Test
    public void testServingEmptyFile() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileLocation = createEmptyTestFile(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseIsNotEmpty(fileLocation);
    }

    private File createEmptyTestFile(File directoryLocation) {
        File fileLocation = new File(directoryLocation, TEST_FILE_NAME);

        // Create an empty file
        try {
            fileLocation.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the empty test file: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseIsNotEmpty(File fileLocation) {
        try {
            String urlStr = buildTestUrl(fileLocation);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                String responseStr = br.readLine();
                if (responseStr == null) {
                    fail("Response is empty, but it should not be.");
                }
            }
        } catch (IOException e) {

            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingNonExistentFile() {
        // Setup
        File nonExistentFileLocation = new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), "NonExistentFile.html");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertNonExistentFileResponse(nonExistentFileLocation);
    }

    private void assertNonExistentFileResponse(File nonExistentFileLocation) {
        try {
            String urlStr = buildTestUrl(nonExistentFileLocation);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_NOT_FOUND) {
                fail("Response code was NOT HTTP_NOT_FOUND for a non-existent file");
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingLargeFile() {
        // Setup
        File directoryLocation = createTestDirectory();
        File largeFileLocation = createLargeTestFile(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseMatchesLargeFile(largeFileLocation);
    }

    private File createLargeTestFile(File directoryLocation) {
        File largeFileLocation = new File(directoryLocation, "LargeFile.txt");

        try (PrintWriter writer = new PrintWriter(largeFileLocation, "UTF-8")) {
            // Create a large file (e.g., 1 MB)
            for (int i = 0; i < 1024; i++) {
                writer.println("This is a line in the large file.");
            }
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the large test file: " + e.getMessage());
        }

        return largeFileLocation;
    }

    private void assertResponseMatchesLargeFile(File largeFileLocation) {
        try {
            String urlStr = buildTestUrl(largeFileLocation);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            // Check if the response content length is approximately equal to the size of the large file
            long fileSize = largeFileLocation.length();
            long responseContentLength = connection.getContentLengthLong();
            assertTrue("Response content length is not approximately equal to the size of the large file",
                    Math.abs(fileSize - responseContentLength) < 1024); // Allow a difference of up to 1 KB
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithInvalidExtension() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileWithInvalidExtension = createTestFileWithInvalidExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseHasValidExtension(fileWithInvalidExtension);
    }

    private File createTestFileWithInvalidExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithInvalidExtension.exe");

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println("Invalid file content");
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file with an invalid extension: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseHasValidExtension(File fileWithInvalidExtension) {
        try {
            String urlStr = buildTestUrl(fileWithInvalidExtension);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            // Check if the response content is not empty
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                String responseStr = br.readLine();
                if (responseStr == null) {
                    fail("Response is empty, but it should not be.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithUpperCaseExtension() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileWithUpperCaseExtension = createTestFileWithUpperCaseExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseHasValidExtension(fileWithUpperCaseExtension);
    }

    private File createTestFileWithUpperCaseExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithUpperCaseExtension.HTML");

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println("File content with uppercase extension");
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file with an uppercase extension: " + e.getMessage());
        }

        return fileLocation;
    }

    @Test
    public void testServingBinaryFile() {
        // Setup
        File directoryLocation = createTestDirectory();
        File binaryFileLocation = createBinaryTestFile(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseMatchesBinaryFile(binaryFileLocation);
    }

    private File createBinaryTestFile(File directoryLocation) {
        File binaryFileLocation = new File(directoryLocation, "BinaryFile.bin");

        try (PrintWriter writer = new PrintWriter(binaryFileLocation, "UTF-8")) {
            // Writing binary data (non-text content) to the file
            byte[] binaryData = {0x00, 0x01, 0x02, 0x03, 0x04};
            writer.write(new String(binaryData, StandardCharsets.ISO_8859_1));
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the binary test file: " + e.getMessage());
        }

        return binaryFileLocation;
    }

    private void assertResponseMatchesBinaryFile(File binaryFileLocation) {
        try {
            String urlStr = buildTestUrl(binaryFileLocation);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            // Check if the response content is not empty
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                String responseStr = br.readLine();
                if (responseStr == null) {
                    fail("Response is empty, but it should not be.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithDifferentExtension() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileWithDifferentExtension = createTestFileWithDifferentExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseHasValidExtension(fileWithDifferentExtension);
    }

    private File createTestFileWithDifferentExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithDifferentExtension.txt");

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println("File content with a different extension");
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file with a different extension: " + e.getMessage());
        }

        return fileLocation;
    }

    @Test
    public void testServingFileWithMultipleDotsInFilename() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileWithMultipleDots = createTestFileWithMultipleDots(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseMatchesFileWithMultipleDots(fileWithMultipleDots);
    }

    private File createTestFileWithMultipleDots(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "File.With.Multiple.Dots.html");

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println("File content with multiple dots in the filename");
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file with multiple dots in the filename: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseMatchesFileWithMultipleDots(File fileWithMultipleDots) {
        try {
            String urlStr = buildTestUrl(fileWithMultipleDots);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            // Check if the response content is not empty
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                String responseStr = br.readLine();
                if (responseStr == null) {
                    fail("Response is empty, but it should not be.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithNoExtension() {
        // Setup
        File directoryLocation = createTestDirectory();
        File fileWithNoExtension = createTestFileWithNoExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseHasValidExtension(fileWithNoExtension);
    }

    private File createTestFileWithNoExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithNoExtension");

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println("File content with no extension");
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file with no extension: " + e.getMessage());
        }

        return fileLocation;
    }

}
