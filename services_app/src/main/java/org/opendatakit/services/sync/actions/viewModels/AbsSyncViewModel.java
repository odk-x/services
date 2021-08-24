package org.opendatakit.services.sync.actions.viewModels;

import androidx.lifecycle.LiveData;
import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.ViewModel;

import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.SyncAttachmentState;

public class AbsSyncViewModel extends ViewModel {

    private final MutableLiveData<String> appName;
    private final MutableLiveData<Boolean> isFirstLaunch;

    private final MutableLiveData<Boolean> started;
    private final MutableLiveData<SyncAttachmentState> syncAttachmentState;

    private final MutableLiveData<String> serverUrl;
    private final MutableLiveData<Boolean> isServerVerified;
    private final MutableLiveData<Boolean> isAnonymousSignInUsed;
    private final MutableLiveData<Boolean> isAnonymousAllowed;

    private final MutableLiveData<UserState> currentUserState;
    private final MutableLiveData<String> username;
    private final MutableLiveData<Boolean> isUserVerified;
    private final MutableLiveData<Long> lastSyncTime;
    private final MutableLiveData<Boolean> isLastSyncTimeAvailable;

    public AbsSyncViewModel() {
        super();

        appName = new MutableLiveData<>();

        started = new MutableLiveData<>();
        started.setValue(false);

        serverUrl = new MutableLiveData<>();
        isServerVerified = new MutableLiveData<>();
        isAnonymousSignInUsed = new MutableLiveData<>();
        isAnonymousAllowed = new MutableLiveData<>();

        currentUserState = new MutableLiveData<>();
        username = new MutableLiveData<>();
        isUserVerified = new MutableLiveData<>();
        lastSyncTime = new MutableLiveData<>();
        isLastSyncTimeAvailable = new MutableLiveData<>();

        isFirstLaunch = new MutableLiveData<>();

        syncAttachmentState = new MutableLiveData<>();
        syncAttachmentState.setValue(SyncAttachmentState.SYNC);
    }

    public String getAppName() {
        return appName.getValue();
    }

    public boolean getStarted() {
        if (started.getValue() == null) {
            started.setValue(false);
        }
        return started.getValue();
    }

    public LiveData<String> getServerUrl() {
        return serverUrl;
    }

    public String getUrl() {
        return serverUrl.getValue();
    }

    public LiveData<Boolean> checkIsServerVerified() {
        return isServerVerified;
    }

    public LiveData<Boolean> checkIsAnonymousSignInUsed() {
        return isAnonymousSignInUsed;
    }

    public boolean isAnonymousMethodUsed() {
        if (isAnonymousSignInUsed.getValue() != null) {
            return isAnonymousSignInUsed.getValue();
        }
        return false;
    }

    public boolean isAnonymousAllowed() {
        if(isAnonymousAllowed.getValue()!=null){
            return isAnonymousAllowed.getValue();
        }
        return true;
    }

    public LiveData<Boolean> checkIsAnonymousAllowed() {
        return isAnonymousAllowed;
    }

    public LiveData<UserState> getCurrentUserState() {
        return currentUserState;
    }

    public UserState getUserState() {
        return currentUserState.getValue();
    }

    public LiveData<String> getUsername() {
        return username;
    }

    public LiveData<Boolean> checkIsUserVerified() {
        return isUserVerified;
    }

    public LiveData<Long> getLastSyncTime() {
        return lastSyncTime;
    }

    public LiveData<Boolean> checkIsLastSyncTimeAvailable() {
        return isLastSyncTimeAvailable;
    }

    public LiveData<Boolean> checkIsFirstLaunch() {
        return isFirstLaunch;
    }

    public LiveData<SyncAttachmentState> getSyncAttachmentState() {
        return syncAttachmentState;
    }

    public SyncAttachmentState getCurrentSyncAttachmentState() {
        return syncAttachmentState.getValue();
    }

    public void setAppName(String appName) {
        this.appName.setValue(appName);
    }

    public void setStarted(boolean start) {
        started.setValue(start);
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl.setValue(serverUrl);
    }

    public void setIsServerVerified(boolean isServerVerified) {
        this.isServerVerified.setValue(isServerVerified);
    }

    public void setIsAnonymousSignInUsed(boolean isAnonymousSignInUsed) {
        this.isAnonymousSignInUsed.setValue(isAnonymousSignInUsed);
    }

    public void setIsAnonymousAllowed(boolean isAnonymousAllowed) {
        this.isAnonymousAllowed.setValue(isAnonymousAllowed);
    }

    public void setCurrentUserState(UserState currentUserState) {
        this.currentUserState.setValue(currentUserState);
    }

    public void setUsername(String username) {
        this.username.setValue(username);
    }

    public void setIsUserVerified(boolean isUserVerified) {
        this.isUserVerified.setValue(isUserVerified);
    }

    public void setLastSyncTime(long lastSyncTime) {
        this.lastSyncTime.setValue(lastSyncTime);
    }

    public void setIsLastSyncTimeAvailable(boolean isAvailable) {
        this.isLastSyncTimeAvailable.setValue(isAvailable);
    }

    public void setIsFirstLaunch(boolean isFirstLaunch) {
        this.isFirstLaunch.setValue(isFirstLaunch);
    }

    public void updateSyncAttachmentState(SyncAttachmentState attachmentState) {
        syncAttachmentState.setValue(attachmentState);
    }
}
