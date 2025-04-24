# Services

This project is __*actively maintained*__

It is part of the ODK-X Android tools suite.

Prior to rev 200, this repo was the __*core*__ repo.

It is an APK that provides services (database, content providers, local webserver) used by all the other ODK-X tools.

The developer [wiki](https://github.com/odk-x/tool-suite-X/wiki) (including release notes) and [issues tracker](https://github.com/odk-x/tool-suite-X/issues) are located under the [**ODK-X Tool Suite**](https://github.com/odk-x) project.

Engage with the community and get technical support on [the ODK-X forum](https://forum.odk-x.org)

## Setting up your environment

General instructions for setting up an ODK-X environment can be found at our [Developer Environment Setup wiki page](https://github.com/odk-x/tool-suite-X/wiki/Developer-Environment-Setup)

Install [Android Studio](http://developer.android.com/tools/studio/index.html) and the [SDK](http://developer.android.com/sdk/index.html#Other).


This project depends on ODK-X's [androidlibrary](https://github.com/odk-x/androidlibrary) project; its binaries will be downloaded automatically from our maven repository during the build phase. If you wish to modify them yourself, you must clone them into the same parent directory as survey. You directory structure should resemble the following:

        |-- odk-x
        
            |-- services

            |-- androidlibrary


  * Note that this only applies if you are modifying androidlibrary. If you use the maven dependencies (the default option), the project will not show up in your directory.

Now you should be ready to build.

## Building the project

Open the Services project in Android Studio. Select `Build->Make Project' to build the app.

## Running

If the project builds properly, it should be able to run on an Android device without any other prerequisites.

## Source tree information
Quick description of the content in the root folder:

    |-- services_app     -- Source tree for Java components

        |-- src

            |-- main

                |-- res     -- Source tree for Android resources

                |-- java

                    |-- org

                        |-- opendatakit  -- The most relevant Java code lives here

            |-- androidTest     -- Source tree for Android implementation tests
            |-- test            -- Source tree for Java JUnit tests

## How to contribute
If you’re new to ODK-X you can check out the documentation:
- [https://docs.odk-x.org](https://docs.odk-x.org)

Once you’re up and running, you can choose an issue to start working on from here: 
- [https://github.com/odk-x/tool-suite-X/issues](https://github.com/odk-x/tool-suite-X/issues)

If you're writing tests, we use JaCoCo for test coverage reporting. This is already included in [gradle-config](https://github.com/odk-x/gradle-config)

Issues tagged as [good first issue](https://github.com/odk-x/tool-suite-X/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) should be a good place to start.

Pull requests are welcome, though please submit them against the development branch. We prefer verbose descriptions of the change you are submitting. If you are fixing a bug, please provide steps to reproduce it or a link to an issue that provides that information. If you are submitting a new feature, please provide a description of the need or a link to a forum discussion about it.

## Running and viewing test coverage 
- Clone [gradle-config](https://github.com/odk-x/gradle-config) into your local repo's parent folder
- Build the project and ensure success.
- Run coverage with `./gradlew createSnapshotDebugUnitTestCoverageReport`.
- To view the report, open the file at ( .../services_app/build/reports/coverage/test/snapshot/debug/index.html) in browser.

## Links for users
This document is aimed at helping developers and technical contributors. For information on how to get started as a user of ODK-X, see our [online documentation](https://docs.odk-x.org), or to learn more about the Open Data Kit project, visit [https://odk-x.org](https://odk-x.org).
