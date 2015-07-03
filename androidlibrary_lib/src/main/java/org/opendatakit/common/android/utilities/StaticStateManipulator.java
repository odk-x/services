/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.common.android.utilities;

import java.util.ArrayList;

/**
 * Classes that maintain non-final static state should register an
 * IStaticFieldManipulator implementation with the singleton instance
 * of this class so that the testing framework can reset this non-final
 * static state to its initial value.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class StaticStateManipulator {
  
  private static StaticStateManipulator gStateManipulator = new StaticStateManipulator();
  
  /**
   * Used only by the test framework to obtain the 
   * singleton instance that can reset all static
   * state in an APK.
   * 
   * @return
   */
  public static synchronized StaticStateManipulator get() {
    return gStateManipulator;
  }

  /**
   * Used for mocking only. Normally never called.
   * 
   * @param sm
   */
  public static synchronized void set(StaticStateManipulator sm) {
    gStateManipulator = sm;
  }

  /**
   * Implement this interface to enable the test framework to reset the 
   * state of a non-final static field to its initial state.
   *  
   * @author mitchellsundt@gmail.com
   *
   */
  public interface IStaticFieldManipulator {
    
    /**
     * Invoked by the test framework when resetting
     * non-final static fields to their initial state.
     * 
     * Implementers are expected to perform this 
     * resetting action (however they see fit).
     */
    public void reset();
  }

  private final ArrayList<IStaticFieldManipulator> mStaticManipulators = new ArrayList<IStaticFieldManipulator>();

  protected StaticStateManipulator() {
  }
  
  /**
   * Reset all non-final static state in the application 
   * to its initial values. Call this during testing to
   * ensure that a test case starts 'clean' (except w.r.t.
   * data files on the device).
   */
  public void reset() {
    for ( IStaticFieldManipulator fm : mStaticManipulators ) {
      fm.reset();
    }
  }
  
  /**
   * Invoke this to register a class that can reset 
   * non-final static fields to their initial state.
   * 
   * @param order The order in which to invoke this relative to
   *     other static field manipulators. Use 50 if you are not
   *     aware of a conflict or if there is no state within the 
   *     object being reset (e.g., it is only there to support
   *     mocking its interface for other unit tests). When you 
   *     add a manipulator, please record the order and the 
   *     manipulated class here.  
   *     <ul>
   *     <li>50 -- ColumnUtil</li>
   *     <li>50 -- DatabaseFactory</li>
   *     <li>50 -- GeoColumnUtil</li>
   *     <li>50 -- ODKDatabaseUtils</li>
   *     <li>50 -- RowPathColumnUtil</li>
   *     <li>50 -- TableUtil</li>
   *     <li>50 -- WebUtils</li>
   *     <li>50 -- PropertiesSingletonFactory</li>
   *     <li>75 -- ClientConnectionManagerFactory</li>
   *     <li>75 -- ODKWebChromeClient (Survey)</li>
   *     <li>90 -- ElementTypeManipulatorFactory (Tables)</li>
   *     <li>90 -- PropertiesSingleton (Survey)</li>
   *     <li>99 -- WebLogger (live OutputStreamWriter objects)</li>
   *     </ul>
   * @param fm a manipulator that can perform the reset.
   */
  public void register(int order, IStaticFieldManipulator fm) {
    mStaticManipulators.add(fm);
  }
  
}
