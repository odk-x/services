package org.opendatakit.dbshim.service;

import org.opendatakit.common.android.utilities.WebLogger;

import android.os.RemoteException;

class OdkDbShimServiceInterface extends OdkDbShimInterface.Stub {
  
  private static final String LOGTAG = OdkDbShimServiceInterface.class.getSimpleName();

  /**
   * 
   */
  private final OdkDbShimService odkDbShimService;

  /**
   * @param odkDbShimService
   */
  OdkDbShimServiceInterface(OdkDbShimService odkDbShimService) {
    this.odkDbShimService = odkDbShimService;
  }

  @Override
  public void initializeDatabaseConnections(String appName, String generation, DbShimCallback callback)
      throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG,
          "SERVICE INTERFACE: initializeDatabaseConnections WITH appName:" + appName + " generation: " + generation);
      odkDbShimService.queueInitializeDatabaseConnections(appName, generation, callback);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public void runCommit(String appName, String generation, int transactionGeneration, DbShimCallback callback)
      throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG,
          "SERVICE INTERFACE: runCommit WITH appName:" + appName + " generation: " + generation +
          " transactionGeneration: " + transactionGeneration);
      odkDbShimService.queueRunCommit(appName, generation, transactionGeneration, callback);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
  }

  @Override
  public void runRollback(String appName, String generation, int transactionGeneration, DbShimCallback callback)
      throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG,
          "SERVICE INTERFACE: runRollback WITH appName:" + appName + " generation: " + generation +
          " transactionGeneration: " + transactionGeneration);
      odkDbShimService.queueRunRollback(appName, generation, transactionGeneration, callback);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
    
  }

  @Override
  public void runStmt(String appName, String generation, int transactionGeneration, int actionIdx, String sqlStmt, String strBinds,
      DbShimCallback callback) throws RemoteException {
    try {
      WebLogger.getLogger(appName).i(LOGTAG,
          "SERVICE INTERFACE: runStmt WITH appName:" + appName + " generation: " + generation +
          " transactionGeneration: " + transactionGeneration + " actionIdx: " + actionIdx);
      odkDbShimService.queueRunStmt(appName, generation, transactionGeneration, actionIdx, sqlStmt, strBinds, callback);
    } catch (Throwable throwable) {
      WebLogger.getLogger(appName).printStackTrace(throwable);
      throw new RemoteException();
    }
    
  }

}