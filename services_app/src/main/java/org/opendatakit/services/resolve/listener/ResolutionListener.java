package org.opendatakit.services.resolve.listener;

/**
 * @author mitchellsundt@gmail.com
 */
public interface ResolutionListener {
  /**
   * Reports intermediate progress messages to UI
   * @param progress
   */
  void resolutionProgress(String progress);

  /**
   * Reports completion of action to UI
   * @param result
   */
  void resolutionComplete(String result);
}
