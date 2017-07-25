/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.application;

import android.content.Context;
import org.opendatakit.application.ToolAwareApplication;
import org.opendatakit.services.R;

import java.lang.ref.WeakReference;

/**
 * The application object
 */
public class Services extends ToolAwareApplication {
  private static WeakReference<Services> ref = null;

  @Override
  public void onCreate() {
    super.onCreate();
    ref = new WeakReference<>(this);
  }

  @Deprecated
  public static Context _please_dont_use_getInstance() {
    if (ref == null || ref.get() == null)
      return null;
    return ref.get().getApplicationContext();
  }

  @Override public int getApkDisplayNameResourceId() {
    return R.string.app_name;
  }
}
