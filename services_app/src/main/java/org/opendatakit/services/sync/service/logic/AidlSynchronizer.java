package org.opendatakit.services.sync.service.logic;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.net.Uri;
import android.os.IBinder;
import android.os.RemoteException;

import androidx.annotation.VisibleForTesting;

import org.opendatakit.aggregate.odktables.rest.entity.ChangeSetList;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.PrivilegesInfo;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.UserInfoList;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.HttpClientWebException;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.entity.ParcelableColumn;
import org.opendatakit.sync.service.entity.ParcelableTableResource;
import org.opendatakit.sync.service.logic.CommonFileAttachmentTerms;
import org.opendatakit.sync.service.logic.FileManifestDocument;
import org.opendatakit.sync.service.logic.IAidlSynchronizer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AidlSynchronizer implements Synchronizer {
  private static final int SERVICE_CHECK_INTERVAL = 100;
  private static final String TAG = AidlSynchronizer.class.getSimpleName();

  private IAidlSynchronizer remoteInterface;

  public IAidlSynchronizer getRemoteInterface() {
    while (remoteInterface == null) {
      try {
        // TODO: Use Handler
        Thread.sleep(SERVICE_CHECK_INTERVAL);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    return remoteInterface;
  }

  public AidlSynchronizer(final SyncExecutionContext ctx, String pkgName, String className) {
    Intent intent = new Intent()
        .setClassName(pkgName, className)
        // TODO: make a new class that holds sync parameters? SyncExecutionContext is too big
        .putExtra(IntentConsts.INTENT_KEY_APP_NAME, ctx.getAppName());

    ctx.getApplication().bindService(intent, new ServiceConnection() {
      @Override
      public void onServiceConnected(ComponentName name, IBinder service) {
        remoteInterface = IAidlSynchronizer.Stub.asInterface(service);
      }

      @Override
      public void onServiceDisconnected(ComponentName name) {
        remoteInterface = null;
        ctx.getApplication().unbindService(this);
      }
    }, Context.BIND_AUTO_CREATE);
  }

  @Override
  public void verifyServerSupportsAppName() throws HttpClientWebException {
    try {
      getRemoteInterface().verifyServerSupportsAppName();
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public PrivilegesInfo getUserRolesAndDefaultGroup() throws HttpClientWebException {
    try {
      return getRemoteInterface().getUserRolesAndDefaultGroup();
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public UserInfoList getUsers() throws HttpClientWebException {
    try {
      return getRemoteInterface().getUsers();
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public URI constructAppLevelFileManifestUri() throws HttpClientWebException {
    try {
      return new URI(getRemoteInterface().constructAppLevelFileManifestUri().toString());
    } catch (URISyntaxException | RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public URI constructTableLevelFileManifestUri(String tableId) throws HttpClientWebException {
    try {
      return new URI(getRemoteInterface().constructTableLevelFileManifestUri(tableId).toString());
    } catch (URISyntaxException | RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public URI constructRealizedTableIdUri(String tableId, String schemaETag) throws HttpClientWebException {
    try {
      return new URI(getRemoteInterface().constructRealizedTableIdUri(tableId, schemaETag).toString());
    } catch (URISyntaxException | RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public URI constructInstanceFileManifestUri(String serverInstanceFileUri, String rowId) throws HttpClientWebException {
    try {
      return new URI(getRemoteInterface().constructInstanceFileManifestUri(serverInstanceFileUri, rowId).toString());
    } catch (URISyntaxException | RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public TableResourceList getTables(String webSafeResumeCursor) throws HttpClientWebException {
    try {
      return getRemoteInterface().getTables(webSafeResumeCursor);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public TableResource getTable(String tableId) throws HttpClientWebException {
    try {
      return getRemoteInterface().getTable(tableId);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public TableDefinitionResource getTableDefinition(String tableDefinitionUri) throws HttpClientWebException {
    try {
      return getRemoteInterface().getTableDefinition(tableDefinitionUri);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns) throws HttpClientWebException {
    List<ParcelableColumn> pColumns = new ArrayList<>();
    for (Column column : columns) {
      pColumns.add(((ParcelableColumn) column));
    }

    try {
      return getRemoteInterface().createTable(tableId, schemaETag, pColumns);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public void deleteTable(TableResource table) throws HttpClientWebException {
    try {
      getRemoteInterface().deleteTable(((ParcelableTableResource) table));
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public ChangeSetList getChangeSets(TableResource tableResource, String dataETag) throws HttpClientWebException {
    try {
      return getRemoteInterface().getChangeSets(((ParcelableTableResource) tableResource), dataETag);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public RowResourceList getChangeSet(TableResource tableResource, String dataETag, boolean activeOnly, String websafeResumeCursor) throws HttpClientWebException {
    try {
      return getRemoteInterface().getChangeSet(((ParcelableTableResource) tableResource), dataETag, activeOnly, websafeResumeCursor);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public RowResourceList getUpdates(TableResource tableResource, String dataETag, String websafeResumeCursor, int fetchLimit) throws HttpClientWebException {
    try {
      // TODO: handle TransactionTooLargeException
      return getRemoteInterface().getUpdates(((ParcelableTableResource) tableResource), dataETag, websafeResumeCursor, fetchLimit);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public RowOutcomeList pushLocalRows(TableResource tableResource, OrderedColumns orderedColumns, List<TypedRow> rowsToInsertUpdateOrDelete) throws HttpClientWebException {
    try {
      // convert List<TypedRow> to List<String>
      // Individual Row/TypedRow instances are not Parcelable
      List<String> rowIds = new ArrayList<>();
      for (TypedRow typedRow : rowsToInsertUpdateOrDelete) {
        rowIds.add(typedRow.getRawStringByKey(DataTableColumns.ID));
      }

      return getRemoteInterface().pushLocalRows(((ParcelableTableResource) tableResource), orderedColumns, rowIds);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public FileManifestDocument getAppLevelFileManifest(String lastKnownLocalAppLevelManifestETag, String serverReportedAppLevelETag, boolean pushLocalFiles) throws HttpClientWebException {
    try {
      return getRemoteInterface().getAppLevelFileManifest(lastKnownLocalAppLevelManifestETag, serverReportedAppLevelETag, pushLocalFiles);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public FileManifestDocument getTableLevelFileManifest(String tableId, String lastKnownLocalTableLevelManifestETag, String serverReportedTableLevelETag, boolean pushLocalFiles) throws HttpClientWebException {
    try {
      return getRemoteInterface().getTableLevelFileManifest(tableId, lastKnownLocalTableLevelManifestETag, serverReportedTableLevelETag, pushLocalFiles);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public FileManifestDocument getRowLevelFileManifest(String serverInstanceFileUri, String tableId, String instanceId, SyncAttachmentState attachmentState, String lastKnownLocalRowLevelManifestETag) throws HttpClientWebException {
    try {
      return getRemoteInterface().getRowLevelFileManifest(serverInstanceFileUri, tableId, instanceId, attachmentState, lastKnownLocalRowLevelManifestETag);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public void downloadFile(File destFile, URI downloadUrl) throws HttpClientWebException {
    try {
      getRemoteInterface().downloadFile(Uri.fromFile(destFile), Uri.parse(downloadUrl.toString()));
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public void deleteConfigFile(File localFile) throws HttpClientWebException {
    try {
      getRemoteInterface().deleteConfigFile(Uri.fromFile(localFile));
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public void uploadConfigFile(File localFile) throws HttpClientWebException {
    try {
      getRemoteInterface().uploadConfigFile(Uri.fromFile(localFile));
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public void uploadInstanceFile(File file, URI instanceFileUri) throws HttpClientWebException {
    try {
      getRemoteInterface().uploadInstanceFile(Uri.fromFile(file), Uri.parse(instanceFileUri.toString()));
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public CommonFileAttachmentTerms createCommonFileAttachmentTerms(String serverInstanceFileUri, String tableId, String instanceId, String rowpathUri) throws HttpClientWebException {
    try {
      return getRemoteInterface().createCommonFileAttachmentTerms(serverInstanceFileUri, tableId, instanceId, rowpathUri);
    } catch (RemoteException e) {
      rethrowException(e);
      throw new IllegalStateException();
    }
  }

  @Override
  public void uploadInstanceFileBatch(List<CommonFileAttachmentTerms> batch, String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException {
    try {
      getRemoteInterface().uploadInstanceFileBatch(batch, serverInstanceFileUri, instanceId, tableId);
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public void downloadInstanceFileBatch(List<CommonFileAttachmentTerms> filesToDownload, String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException {
    try {
      getRemoteInterface().downloadInstanceFileBatch(filesToDownload, serverInstanceFileUri, instanceId, tableId);
    } catch (RemoteException e) {
      rethrowException(e);
    }
  }

  @Override
  public void publishTableSyncStatus(TableResource tableResource, Map<String, Object> statusJSON) {
    // ignore
  }

  @Override
  public void publishDeviceInformation(Map<String, Object> infoJSON) {
    // ignore
  }

  @VisibleForTesting
  public void rethrowException(Throwable e) {
    // TODO:
    throw new HttpClientWebException(e.getMessage(), e, null, null);
  }
}
