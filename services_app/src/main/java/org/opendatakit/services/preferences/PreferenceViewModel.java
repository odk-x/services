package org.opendatakit.services.preferences;

import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.ViewModel;

public class PreferenceViewModel extends ViewModel {
  private MutableLiveData<Boolean> adminConfigured;
  private MutableLiveData<Boolean> adminMode;

  public MutableLiveData<Boolean> getAdminConfigured() {
    if (adminConfigured == null) {
      adminConfigured = new MutableLiveData<>();
    }

    return adminConfigured;
  }

  public void setAdminConfigured(boolean adminConfigured) {
    getAdminConfigured().postValue(adminConfigured);
  }

  public MutableLiveData<Boolean> getAdminMode() {
    if (adminMode == null) {
      adminMode = new MutableLiveData<>();
    }

    return adminMode;
  }

  public void setAdminMode(boolean adminMode) {
    getAdminMode().postValue(adminMode);
  }
}
