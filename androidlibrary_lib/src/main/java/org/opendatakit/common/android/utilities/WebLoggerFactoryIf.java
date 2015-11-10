package org.opendatakit.common.android.utilities;

/**
 * Factory interface for producing WebLoggerIf implementations
 *
 * @author mitchellsundt@gmail.com
 */
public interface WebLoggerFactoryIf {
   public WebLoggerIf createWebLogger(String appName);
}
