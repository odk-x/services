package org.opendatakit.services.preferences;

import android.arch.lifecycle.MutableLiveData;
import android.arch.lifecycle.ViewModel;

public class PreferenceViewModel extends ViewModel {
  private MutableLiveData<Boolean> adminConfigured;
  private MutableLiveData<Boolean> adminMode;
  private MutableLiveData<Boolean> adminRestrictions;

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

  public MutableLiveData<Boolean> getAdminRestrictions() {
    if (adminRestrictions == null) {
      adminRestrictions = new MutableLiveData<>();
    }

    return adminRestrictions;
  }

  public void setAdminRestrictions(boolean adminRestrictions) {
    getAdminRestrictions().postValue(adminRestrictions);
  }
}
