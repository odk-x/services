package org.opendatakit.services.sync.service.logic;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.TableLevelResult;

/**
 * @author mitchellsundt@gmail.com
 */
public abstract class ProcessRowDataSharedBase implements IProcessRowData {

  static final String ID_COLUMN = "id";

  private final WebLoggerIf log;

  final SyncExecutionContext sc;

  private double minPercentage = 0.0;
  private double maxPercentage = 100.0;
  private int totalAffectedRows = 1;
  private Double perRowIncrement = 100.0;

  private int rowsProcessed = 0;

  int maxColumnsToUseLargeFetchLimit = 80;
  int smallFetchLimit = 200;
  int largeFetchLimit = 1000;

  ProcessRowDataSharedBase(SyncExecutionContext sharedContext) {
    this.sc = sharedContext;
    this.log = WebLogger.getLogger(sc.getAppName());
  }

  /**
   * Used for testing.
   *
   * @param maxColumnsToUseSmallFetchLimit
   * @param smallFetchLimit
   * @param largeFetchLimit
   */
  void setFetchLimitParameters(int maxColumnsToUseSmallFetchLimit, int smallFetchLimit, int
      largeFetchLimit) {
    this.maxColumnsToUseLargeFetchLimit = maxColumnsToUseSmallFetchLimit;
    this.smallFetchLimit = smallFetchLimit;
    this.largeFetchLimit = largeFetchLimit;
  }

  public SyncExecutionContext getSyncExecutionContext() {
    return sc;
  }

  WebLoggerIf getLogger() {
    return log;
  }

  void setUpdateNotificationBounds(double minPercentage, double maxPercentage,
      int totalAffectedRows) {

    this.minPercentage = minPercentage;
    this.maxPercentage = maxPercentage;
    this.totalAffectedRows = totalAffectedRows;

    this.perRowIncrement = (maxPercentage - minPercentage) / (totalAffectedRows + 1);
    this.rowsProcessed = 0;
  }

  public void publishUpdateNotification(int idResource, String tableId, double percentage) {
    if ( percentage < 0.0 ) {
      percentage = minPercentage + rowsProcessed * perRowIncrement;
    }
    sc.updateNotification(SyncProgressState.ROWS, idResource,
        new Object[] { tableId },
        percentage, false);
  }

  public void publishUpdateNotification(int idResource, String tableId) {
    ++rowsProcessed;
    sc.updateNotification(SyncProgressState.ROWS, idResource,
        new Object[] { tableId, rowsProcessed, totalAffectedRows },
        minPercentage + rowsProcessed * perRowIncrement, false);
  }

  /**
   * Common error reporting...
   *
   * @param method
   * @param tableId
   * @param e
   * @param tableLevelResult
   */
  void exception(String method, String tableId, Exception e, TableLevelResult tableLevelResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }

    String fmtMsg = String
        .format("Exception in %s on table: %s exception: %s", method, tableId, msg);

    log.e(super.getClass().getSimpleName(), fmtMsg);
    log.printStackTrace(e);

    SyncOutcome outcome = sc.exceptionEquivalentOutcome(e);
    tableLevelResult.setSyncOutcome(outcome);
    tableLevelResult.setMessage(fmtMsg);
  }


}
