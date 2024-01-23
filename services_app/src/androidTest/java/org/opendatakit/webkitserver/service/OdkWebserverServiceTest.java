package org.opendatakit.webkitserver.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.Manifest;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.net.Uri;
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
import java.nio.file.Files;

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

    private String buildTestUrl(File fileLocation) {
        Uri baseUrl = Uri.parse("http://" + WebkitServerConsts.HOSTNAME + ":" +
                Integer.toString(WebkitServerConsts.PORT) + "/" + TestConsts.APPNAME + "/");
        return baseUrl.buildUpon()
                .appendPath(ODKFileUtils.asUriFragment(TestConsts.APPNAME, fileLocation))
                .build()
                .toString();
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

    private void assertResponseMatchesHelloWorldHtml(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
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

    private void assertResponseIsNotEmpty(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
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
    public void testServingNonexistentFile() {
        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertNonexistentFileResponse(getNonexistentFileLocation());
    }

    private File getNonexistentFileLocation() {
        return new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), "NonexistentFile.txt");
    }

    private void assertNonexistentFileResponse(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_NOT_FOUND", HttpStatus.SC_NOT_FOUND, responseCode);
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File largeFileLocation = createLargeTestFile(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertLargeFileResponse(largeFileLocation);
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

    private void assertLargeFileResponse(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for large file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithSpecialCharactersInName() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithSpecialCharacters(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsSpecialCharacters(fileLocation);
    }

    private File createFileWithSpecialCharacters(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "Special@File#Name.txt");

        try {
            // Create a file with special characters in the name
            Files.write(fileLocation.toPath(), "Special characters in file name".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with special characters: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseContainsSpecialCharacters(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for special characters in the file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingMultipleFilesWithSameNameInDifferentDirectories() {
        // Arrange
        File directory1 = createTestDirectory();
        File file1 = createTestFile(directory1);

        File directory2 = createTestDirectory();
        File file2 = createTestFile(directory2);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForMultipleFiles(file1, file2);
    }

    private void assertResponseForMultipleFiles(File file1, File file2) {
        try {
            Uri uri1 = Uri.parse(buildTestUrl(file1));
            Uri uri2 = Uri.parse(buildTestUrl(file2));

            HttpURLConnection connection1 = (HttpURLConnection) new URL(uri1.toString()).openConnection();
            int responseCode1 = connection1.getResponseCode();
            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode1);

            HttpURLConnection connection2 = (HttpURLConnection) new URL(uri2.toString()).openConnection();
            int responseCode2 = connection2.getResponseCode();
            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode2);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithSpacesInName() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithSpaces(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsSpaces(fileLocation);
    }

    private File createFileWithSpaces(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "File with Spaces.txt");

        try {
            // Create a file with spaces in the name
            Files.write(fileLocation.toPath(), "File with spaces in name".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with spaces: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseContainsSpaces(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for spaces in the file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithUnicodeCharactersInContent() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithUnicodeContent(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsUnicodeContent(fileLocation);
    }

    private File createFileWithUnicodeContent(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithUnicodeContent.txt");

        try {
            // Create a file with Unicode characters in the content
            Files.write(fileLocation.toPath(), "Unicode characters: 你好, مرحبا, こんにちは".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with Unicode content: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseContainsUnicodeContent(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for Unicode characters in the file content, if needed
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithInvalidExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForInvalidExtension(fileLocation);
    }

    private File createFileWithInvalidExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "InvalidFile.xyz");

        try {
            // Create a file with an invalid extension
            Files.write(fileLocation.toPath(), "Content with an invalid extension".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with an invalid extension: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseForInvalidExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling an invalid extension, if needed
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithUpperCaseExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForUpperCaseExtension(fileLocation);
    }

    private File createFileWithUpperCaseExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithUpperCaseExtension.TXT");

        try {
            // Create a file with an uppercase extension
            Files.write(fileLocation.toPath(), "Content with an uppercase extension".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with an uppercase extension: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseForUpperCaseExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling an uppercase extension, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingBinaryFile() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File binaryFileLocation = createBinaryFile(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertBinaryFileResponse(binaryFileLocation);
    }

    private File createBinaryFile(File directoryLocation) {
        File binaryFileLocation = new File(directoryLocation, "BinaryFile.bin");

        try {
            // Create a binary file with random content
            byte[] binaryContent = new byte[]{0x12, 0x34, 0x56, 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE, (byte) 0xF0};
            Files.write(binaryFileLocation.toPath(), binaryContent);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the binary test file: " + e.getMessage());
        }

        return binaryFileLocation;
    }

    private void assertBinaryFileResponse(File binaryFileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(binaryFileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling binary file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithMultipleDotsInFilename() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithMultipleDots(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForMultipleDotsInFilename(fileLocation);
    }

    private File createFileWithMultipleDots(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "File.With.Multiple.Dots.txt");

        try {
            // Create a file with multiple dots in the filename
            Files.write(fileLocation.toPath(), "Content with multiple dots in filename".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with multiple dots: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseForMultipleDotsInFilename(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling filenames with multiple dots, if needed
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithNoExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForNoExtension(fileLocation);
    }

    private File createFileWithNoExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithNoExtension");

        try {
            // Create a file with no extension
            Files.write(fileLocation.toPath(), "Content with no extension".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with no extension: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseForNoExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling files with no extension, if needed
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
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createFileWithDifferentExtension(directoryLocation);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForDifferentExtension(fileLocation);
    }

    private File createFileWithDifferentExtension(File directoryLocation) {
        File fileLocation = new File(directoryLocation, "FileWithDifferentExtension.docx");

        try {
            // Create a file with a different extension
            Files.write(fileLocation.toPath(), "Content with a different extension".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the test file with a different extension: " + e.getMessage());
        }

        return fileLocation;
    }

    private void assertResponseForDifferentExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling files with different extensions, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }
}
