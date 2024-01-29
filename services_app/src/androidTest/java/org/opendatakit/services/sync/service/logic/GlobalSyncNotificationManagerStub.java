package org.opendatakit.services.sync.service.logic;

import org.opendatakit.services.sync.service.GlobalSyncNotificationManager;
import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;

final class GlobalSyncNotificationManagerStub implements
        GlobalSyncNotificationManager {

    @Override
    public void startingSync(String appName) throws NoAppNameSpecifiedException {

    }

    @Override
    public void stoppingSync(String appName) throws NoAppNameSpecifiedException {

    }

    @Override
    public void updateNotification(String appName, String text, int maxProgress, int progress,
                                   boolean indeterminateProgress) {

    }

    @Override
    public void finalErrorNotification(String appName, String text) {

    }

    @Override
    public void finalConflictNotification(String appName, String text) {

    }

    @Override
    public void clearNotification(String appName, String title, String text) {

    }

    @Override
    public void clearVerificationNotification(String appName, String title, String text) {

    }
}
