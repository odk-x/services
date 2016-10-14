/*
 * Copyright (C) 2012 University of Washington
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

package org.opendatakit.services.submissions.provider;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.support.annotation.NonNull;
import android.util.Log;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.provider.ProviderConsts;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.DynamicPropertiesCallback;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.properties.PropertyManager;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.KeyValueStoreColumns;
import org.opendatakit.services.utilities.EncryptionUtils;
import org.opendatakit.services.utilities.EncryptionUtils.EncryptedFormInformation;
import org.opendatakit.utilities.FileSet;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.database.service.DbHandle;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * The WebKit does better if there is a content provider vending files to it.
 * This provider vends files under the Forms and Instances directories (only).
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class SubmissionProvider extends ContentProvider {
  private static final String ISO8601_DATE_ONLY_FORMAT = "yyyy-MM-dd";
  private static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

  private static final String t = "SubmissionProvider";

  private static final String XML_OPENROSA_NAMESPACE = "http://openrosa.org/xforms";
  // // any
  // arbitrary
  // namespace
  private static final String NEW_LINE = "\n";

  /**
   * change to true expression if you want to debug this content provider
   */
  private static void possiblyWaitForContentProviderDebugger() {
    if ( false ) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
    }
  }

  @Override
  public boolean onCreate() {

    // IMPORTANT NOTE: the Application object is not yet created!
    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      File f = new File(ODKFileUtils.getOdkFolder());
      if (!f.exists()) {
        f.mkdir();
      } else if (!f.isDirectory()) {
        Log.e(t, f.getAbsolutePath() + " is not a directory!");
        return false;
      }
    } catch (Exception e) {
      Log.e(t, "External storage not available");
      return false;
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  private static void putElementValue(HashMap<String, Object> dataMap, ColumnDefinition defn,
      Object value) {
    List<ColumnDefinition> nesting = new ArrayList<ColumnDefinition>();
    ColumnDefinition cur = defn.getParent();
    while (cur != null) {
      nesting.add(cur);
      cur = cur.getParent();
    }

    HashMap<String, Object> elem = dataMap;
    for (int i = nesting.size() - 1; i >= 0; --i) {
      cur = nesting.get(i);
      if (elem.containsKey(cur.getElementName())) {
        elem = (HashMap<String, Object>) elem.get(cur.getElementName());
      } else {
        elem.put(cur.getElementName(), new HashMap<String, Object>());
        elem = (HashMap<String, Object>) elem.get(cur.getElementName());
      }
    }
    elem.put(defn.getElementName(), value);
  }

  @SuppressWarnings("unchecked")
  private static int generateXmlHelper(Document d, Element data, int idx, String key,
      Map<String, Object> values, WebLoggerIf logger) {
    Object o = values.get(key);

    Element e = d.createElement(key);

    if (o == null) {
      logger.e(t, "Unexpected null value");
    } else if (o instanceof Integer) {
      Text txtNode = d.createTextNode(((Integer) o).toString());
      e.appendChild(txtNode);
    } else if (o instanceof Double) {
      Text txtNode = d.createTextNode(((Double) o).toString());
      e.appendChild(txtNode);
    } else if (o instanceof Boolean) {
      Text txtNode = d.createTextNode(((Boolean) o).toString());
      e.appendChild(txtNode);
    } else if (o instanceof String) {
      Text txtNode = d.createTextNode(((String) o));
      e.appendChild(txtNode);
    } else if (o instanceof List) {
      StringBuilder b = new StringBuilder();
      List<Object> al = (List<Object>) o;
      for (Object ob : al) {
        if (ob instanceof Integer) {
          b.append(((Integer) ob).toString());
        } else if (ob instanceof Double) {
          b.append(((Double) ob).toString());
        } else if (ob instanceof Boolean) {
          b.append(((Boolean) ob).toString());
        } else if (ob instanceof String) {
          b.append(((String) ob));
        } else {
          throw new IllegalArgumentException("Unexpected type in XML submission serializer");
        }
        b.append(" ");
      }
      Text txtNode = d.createTextNode(b.toString().trim());
      e.appendChild(txtNode);
    } else if (o instanceof Map) {
      // it is an object...
      Map<String, Object> m = (Map<String, Object>) o;
      int nidx = 0;

      ArrayList<String> entryNames = new ArrayList<String>();
      entryNames.addAll(m.keySet());
      Collections.sort(entryNames);
      for (String name : entryNames) {
        nidx = generateXmlHelper(d, e, nidx, name, m, logger);
      }
    } else {
      throw new IllegalArgumentException("Unexpected object type in XML submission serializer");
    }
    data.appendChild(e);
    return idx;
  }

  /**
   * The incoming URI is of the form:
   * ..../appName/tableId/instanceId?formId=&formVersion=
   *
   * where instanceId is the DataTableColumns._ID
   */
  @SuppressWarnings("unchecked")
  @Override
  public ParcelFileDescriptor openFile(@NonNull Uri uri, @NonNull String mode) throws FileNotFoundException {

    possiblyWaitForContentProviderDebugger();

    final boolean asXml = uri.getAuthority().equalsIgnoreCase(ProviderConsts.XML_SUBMISSION_AUTHORITY);

    if (mode != null && !mode.equals("r")) {
      throw new IllegalArgumentException("Only read access is supported");
    }

    // URI == ..../appName/tableId/instanceId?formId=&formVersion=

    List<String> segments = uri.getPathSegments();

    if (segments.size() != 4) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of path segments!) " + uri);
    }

    PropertyManager propertyManager = new PropertyManager(getContext());

    final String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLoggerIf logger = WebLogger.getLogger(appName);

    final String tableId = segments.get(1);
    final String instanceId = segments.get(2);
    final String submissionInstanceId = segments.get(3);
    
    PropertiesSingleton props = CommonToolProperties.get(getContext(), appName);
    String userEmail = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
    String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
    String activeUser = props.getActiveUser();
    String rolesList = props.getProperty(CommonToolProperties.KEY_ROLES_LIST);
    String currentLocale = props.getLocale();

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);

      boolean success = false;
      try {
        success = ODKDatabaseImplUtils.get().hasTableId(db, tableId);
      } catch (Exception e) {
        logger.printStackTrace(e);
        throw new SQLException("Unknown URI (exception testing for tableId) " + uri);
      }
      if (!success) {
        throw new SQLException("Unknown URI (missing data table for tableId) " + uri);
      }

      // Get the table properties specific to XML submissions

      String xmlInstanceName = null;
      String xmlRootElementName = null;
      String xmlDeviceIdPropertyName = null;
      String xmlUserIdPropertyName = null;
      String xmlBase64RsaPublicKey = null;

      try {

        Cursor c = null;
        try {
          c = db.query(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, new String[] {
              KeyValueStoreColumns.KEY, KeyValueStoreColumns.VALUE }, KeyValueStoreColumns.TABLE_ID
              + "=? AND " + KeyValueStoreColumns.PARTITION + "=? AND "
              + KeyValueStoreColumns.ASPECT + "=? AND " + KeyValueStoreColumns.KEY
              + " IN (?,?,?,?,?)", new String[] { tableId, KeyValueStoreConstants.PARTITION_TABLE,
              KeyValueStoreConstants.ASPECT_DEFAULT, KeyValueStoreConstants.XML_INSTANCE_NAME,
              KeyValueStoreConstants.XML_ROOT_ELEMENT_NAME,
              KeyValueStoreConstants.XML_DEVICE_ID_PROPERTY_NAME,
              KeyValueStoreConstants.XML_USER_ID_PROPERTY_NAME,
              KeyValueStoreConstants.XML_BASE64_RSA_PUBLIC_KEY }, null, null, null, null);
          c.moveToFirst();

          if (c.getCount() > 0) {
            int idxKey = c.getColumnIndex(KeyValueStoreColumns.KEY);
            int idxValue = c.getColumnIndex(KeyValueStoreColumns.VALUE);
            do {
              String key = c.getString(idxKey);
              String value = c.getString(idxValue);
              if (KeyValueStoreConstants.XML_INSTANCE_NAME.equals(key)) {
                xmlInstanceName = value;
              } else if (KeyValueStoreConstants.XML_ROOT_ELEMENT_NAME.equals(key)) {
                xmlRootElementName = value;
              } else if (KeyValueStoreConstants.XML_DEVICE_ID_PROPERTY_NAME.equals(key)) {
                xmlDeviceIdPropertyName = value;
              } else if (KeyValueStoreConstants.XML_USER_ID_PROPERTY_NAME.equals(key)) {
                xmlUserIdPropertyName = value;
              } else if (KeyValueStoreConstants.XML_BASE64_RSA_PUBLIC_KEY.equals(key)) {
                xmlBase64RsaPublicKey = value;
              }
            } while (c.moveToNext());
          }
        } finally {
          c.close();
          c = null;
        }

        OrderedColumns orderedDefns = ODKDatabaseImplUtils.get()
            .getUserDefinedColumns(db, tableId);

        // Retrieve the values of the record to be emitted...

        HashMap<String, Object> values = new HashMap<String, Object>();

        // issue query to retrieve the most recent non-checkpoint data record
        // for the instanceId
        StringBuilder b = new StringBuilder();
        b.append("SELECT * FROM ").append(tableId).append(" as T WHERE ")
            .append(DataTableColumns.ID).append("=?").append(" AND ")
            .append(DataTableColumns.SAVEPOINT_TYPE).append(" IS NOT NULL AND ")
            .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append("=(SELECT max(V.")
                .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(") FROM ").append(tableId)
                   .append(" as V WHERE V.").append(DataTableColumns.ID).append("=T.")
                   .append(DataTableColumns.ID).append(" AND V.")
                   .append(DataTableColumns.SAVEPOINT_TYPE).append(" IS NOT NULL").append(")");

        String[] selectionArgs = new String[] { instanceId };
        FileSet freturn = new FileSet(appName);

        String datestamp = null;

        try {

          ODKDatabaseImplUtils.AccessContext accessContext =
              ODKDatabaseImplUtils.get().getAccessContext(db, tableId, activeUser,
                  rolesList);

          c = ODKDatabaseImplUtils.get().rawQuery(db, b.toString(), selectionArgs, null,
              accessContext);
          b.setLength(0);

          if (c.moveToFirst() && c.getCount() == 1) {
            String rowETag = null;
            String filterType = null;
            String filterValue = null;
            String formId = null;
            String locale = null;
            String savepointType = null;
            String savepointCreator = null;
            String savepointTimestamp = null;
            String instanceName = null;

            // OK. we have the record -- work through all the terms
            for (int i = 0; i < c.getColumnCount(); ++i) {
              ColumnDefinition defn = null;
              String columnName = c.getColumnName(i);
              try {
                defn = orderedDefns.find(columnName);
              } catch (IllegalArgumentException e) {
                // ignore...
              }
              if (defn != null && !c.isNull(i)) {
                if (xmlInstanceName != null && defn.getElementName().equals(xmlInstanceName)) {
                  instanceName = CursorUtils.getIndexAsString(c, i);
                }
                // user-defined column
                ElementType type = defn.getType();
                ElementDataType dataType = type.getDataType();

                logger.i(t, "element type: " + defn.getElementType());
                if (dataType == ElementDataType.integer) {
                  Integer value = CursorUtils.getIndexAsType(c, Integer.class, i);
                  putElementValue(values, defn, value);
                } else if (dataType == ElementDataType.number) {
                  Double value = CursorUtils.getIndexAsType(c, Double.class, i);
                  putElementValue(values, defn, value);
                } else if (dataType == ElementDataType.bool) {
                  Integer tmp = CursorUtils.getIndexAsType(c, Integer.class, i);
                  Boolean value = tmp == null ? null : (tmp != 0);
                  putElementValue(values, defn, value);
                } else if (type.getElementType().equals("date")) {
                  String value = CursorUtils.getIndexAsString(c, i);
                  String jrDatestamp = (value == null) ? null : (new SimpleDateFormat(
                      ISO8601_DATE_ONLY_FORMAT, Locale.US)).format(new Date(TableConstants
                      .milliSecondsFromNanos(value)));
                  putElementValue(values, defn, jrDatestamp);
                } else if (type.getElementType().equals("dateTime")) {
                  String value = CursorUtils.getIndexAsString(c, i);
                  String jrDatestamp = (value == null) ? null : (new SimpleDateFormat(
                      ISO8601_DATE_FORMAT, Locale.US)).format(new Date(TableConstants
                      .milliSecondsFromNanos(value)));
                  putElementValue(values, defn, jrDatestamp);
                } else if (type.getElementType().equals("time")) {
                  String value = CursorUtils.getIndexAsString(c, i);
                  putElementValue(values, defn, value);
                } else if (dataType == ElementDataType.array) {
                  ArrayList<Object> al = CursorUtils.getIndexAsType(c, ArrayList.class,
                      i);
                  putElementValue(values, defn, al);
                } else if (dataType == ElementDataType.string) {
                  String value = CursorUtils.getIndexAsString(c, i);
                  putElementValue(values, defn, value);
                } else /* unrecognized */{
                  throw new IllegalStateException("unrecognized data type: "
                      + defn.getElementType());
                }

              } else if (columnName.equals(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
                savepointTimestamp = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.ROW_ETAG)) {
                rowETag = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.FILTER_TYPE)) {
                filterType = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.FILTER_VALUE)) {
                filterValue = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.FORM_ID)) {
                formId = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.LOCALE)) {
                locale = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.FORM_ID)) {
                formId = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.SAVEPOINT_TYPE)) {
                savepointType = CursorUtils.getIndexAsString(c, i);
              } else if (columnName.equals(DataTableColumns.SAVEPOINT_CREATOR)) {
                savepointCreator = CursorUtils.getIndexAsString(c, i);
              }
            }

            // OK got all the values into the values map -- emit
            // contents
            b.setLength(0);
            File submissionXml = new File(ODKFileUtils.getInstanceFolder(appName, tableId,
                instanceId), (asXml ? "submission.xml" : "submission.json"));
            File manifest = new File(ODKFileUtils.getInstanceFolder(appName, tableId, instanceId),
                "manifest.json");
            submissionXml.delete();
            manifest.delete();
            freturn.instanceFile = submissionXml;

            if (asXml) {
              // Pre-processing -- collapse all geopoints into a
              // string-valued representation
              for (ColumnDefinition defn : orderedDefns.getColumnDefinitions()) {
                ElementType type = defn.getType();
                ElementDataType dataType = type.getDataType();
                if (dataType == ElementDataType.object
                    && (type.getElementType().equals("geopoint") || type.getElementType().equals(
                        "mimeUri"))) {
                  Map<String, Object> parent = null;
                  List<ColumnDefinition> parents = new ArrayList<ColumnDefinition>();
                  ColumnDefinition d = defn.getParent();
                  while (d != null) {
                    parents.add(d);
                    d = d.getParent();
                  }
                  parent = values;
                  for (int i = parents.size() - 1; i >= 0; --i) {
                    Object o = parent.get(parents.get(i).getElementName());
                    if (o == null) {
                      parent = null;
                      break;
                    }
                    parent = (Map<String, Object>) o;
                  }
                  if (parent != null) {
                    Object o = parent.get(defn.getElementName());
                    if (o != null) {
                      if (type.getElementType().equals("geopoint")) {
                        Map<String, Object> geopoint = (Map<String, Object>) o;
                        // OK. we have geopoint -- get the
                        // lat, long, alt, etc.
                        Double latitude = (Double) geopoint.get("latitude");
                        Double longitude = (Double) geopoint.get("longitude");
                        Double altitude = (Double) geopoint.get("altitude");
                        Double accuracy = (Double) geopoint.get("accuracy");
                        String gpt = "" + latitude + " " + longitude + " " + altitude + " "
                            + accuracy;
                        parent.put(defn.getElementName(), gpt);
                      } else if (type.getElementType().equals("mimeUri")) {
                        Map<String, Object> mimeuri = (Map<String, Object>) o;
                        String uriFragment = (String) mimeuri.get("uriFragment");
                        String contentType = (String) mimeuri.get("contentType");

                        if (uriFragment != null) {
                          File f = ODKFileUtils.getAsFile(appName, uriFragment);
                          if (f.equals(manifest)) {
                            throw new IllegalStateException(
                                "Unexpected collision with manifest.json");
                          }
                          freturn.addAttachmentFile(f, contentType);
                          parent.put(defn.getElementName(), f.getName());
                        }
                      } else {
                        throw new IllegalStateException("Unhandled transform case");
                      }
                    }
                  }
                }
              }

              datestamp = (new SimpleDateFormat(ISO8601_DATE_FORMAT, Locale.US))
                  .format(new Date(TableConstants.milliSecondsFromNanos(savepointTimestamp)));

              // For XML, we traverse the map to serialize it
              DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
              DocumentBuilder docBuilder = dbf.newDocumentBuilder();
              
              Document d = docBuilder.newDocument();

              d.setXmlStandalone(true);
              
              Element e = d.createElement(
                  (xmlRootElementName == null) ? "data" : xmlRootElementName);
              d.appendChild(e);
              e.setAttribute("id", tableId);
              DynamicPropertiesCallback cb = new DynamicPropertiesCallback(appName,
                  tableId, instanceId, activeUser, currentLocale, username, userEmail);

              int idx = 0;
              Element meta = d.createElementNS(XML_OPENROSA_NAMESPACE, "meta");
              meta.setPrefix("jr");

              Element v = d.createElementNS(XML_OPENROSA_NAMESPACE, "instanceID");
              Text txtNode = d.createTextNode(submissionInstanceId);
              v.appendChild(txtNode);
              meta.appendChild(v);

              if (xmlDeviceIdPropertyName != null) {
                String deviceId = propertyManager.getSingularProperty(xmlDeviceIdPropertyName, cb);
                if (deviceId != null) {
                  v = d.createElementNS(XML_OPENROSA_NAMESPACE, "deviceID");
                  txtNode = d.createTextNode(deviceId);
                  v.appendChild(txtNode);
                  meta.appendChild(v);
                }
              }
              if (xmlUserIdPropertyName != null) {
                String userId = propertyManager.getSingularProperty(xmlUserIdPropertyName, cb);
                if (userId != null) {
                  v = d.createElementNS(XML_OPENROSA_NAMESPACE, "userID");
                  txtNode = d.createTextNode(userId);
                  v.appendChild(txtNode);
                  meta.appendChild(v);
                }
              }
              v = d.createElementNS(XML_OPENROSA_NAMESPACE, "timeEnd");
              txtNode = d.createTextNode(datestamp);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // these are extra metadata tags...
              if (instanceName != null) {
                v = d.createElement("instanceName");
                txtNode = d.createTextNode(instanceName);
                v.appendChild(txtNode);
                meta.appendChild(v);
              } else {
                v = d.createElement("instanceName");
                txtNode = d.createTextNode(savepointTimestamp);
                v.appendChild(txtNode);
                meta.appendChild(v);
              }

              // these are extra metadata tags...
              // rowID
              v = d.createElement("rowID");
              txtNode = d.createTextNode(instanceId);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // rowETag
              v = d.createElement("rowETag");
              if (rowETag != null) {
                txtNode = d.createTextNode(rowETag);
                v.appendChild(txtNode);
              }
              meta.appendChild(v);

              // filterType
              v = d.createElement("filterType");
              if (filterType != null) {
                txtNode = d.createTextNode(filterType);
                v.appendChild(txtNode);
              }
              meta.appendChild(v);

              // filterValue
              v = d.createElement("filterValue");
              if (filterValue != null) {
                txtNode = d.createTextNode(filterValue);
                v.appendChild(txtNode);
              }
              meta.appendChild(v);

              // formID
              v = d.createElement("formID");
              txtNode = d.createTextNode(formId);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // locale
              v = d.createElement("locale");
              txtNode = d.createTextNode(locale);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // savepointType
              v = d.createElement("savepointType");
              txtNode = d.createTextNode(savepointType);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // savepointCreator
              v = d.createElement("savepointCreator");
              if (savepointCreator != null) {
                txtNode = d.createTextNode(savepointCreator);
                v.appendChild(txtNode);
              }
              meta.appendChild(v);

              // savepointTimestamp
              v = d.createElement("savepointTimestamp");
              txtNode = d.createTextNode(savepointTimestamp);
              v.appendChild(txtNode);
              meta.appendChild(v);

              // and insert the meta block into the XML

              e.appendChild(meta);

              idx = 3;
              ArrayList<String> entryNames = new ArrayList<String>();
              entryNames.addAll(values.keySet());
              Collections.sort(entryNames);
              for (String name : entryNames) {
                idx = generateXmlHelper(d, e, idx, name, values, logger);
              }

              TransformerFactory factory = TransformerFactory.newInstance();
              Transformer transformer = factory.newTransformer();
              Properties outFormat = new Properties();
              outFormat.setProperty( OutputKeys.INDENT, "no" );
              outFormat.setProperty( OutputKeys.METHOD, "xml" );
              outFormat.setProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
              outFormat.setProperty( OutputKeys.VERSION, "1.0" );
              outFormat.setProperty( OutputKeys.ENCODING, "UTF-8" );
              transformer.setOutputProperties( outFormat );

              ByteArrayOutputStream out = new ByteArrayOutputStream();

              DOMSource domSource = new DOMSource( d.getDocumentElement() );
              StreamResult result = new StreamResult( out );
              transformer.transform( domSource, result );

              out.flush();
              out.close();

              b.append(out.toString(CharEncoding.UTF_8));

              // OK we have the document in the builder (b).
              String doc = b.toString();

              freturn.instanceFile = submissionXml;

              // see if the form is encrypted and we can
              // encrypt it...
              EncryptedFormInformation formInfo = EncryptionUtils.getEncryptedFormInformation(
                  appName, tableId, xmlBase64RsaPublicKey, instanceId);
              if (formInfo != null) {
                File submissionXmlEnc = new File(submissionXml.getParentFile(),
                    submissionXml.getName() + ".enc");
                submissionXmlEnc.delete();
                // if we are encrypting, the form cannot be
                // reopened afterward
                // and encrypt the submission (this is a
                // one-way operation)...
                if (!EncryptionUtils.generateEncryptedSubmission(freturn, doc, submissionXml,
                    submissionXmlEnc, formInfo)) {
                  return null;
                }
                // at this point, the freturn object has
                // been re-written with the encrypted media
                // and xml files.
              } else {
                exportFile(doc, submissionXml, logger);
              }

            } else {
              // Pre-processing -- collapse all mimeUri into filename
              for (ColumnDefinition defn : orderedDefns.getColumnDefinitions()) {
                ElementType type = defn.getType();
                ElementDataType dataType = type.getDataType();

                if (dataType == ElementDataType.object && type.getElementType().equals("mimeUri")) {
                  Map<String, Object> parent = null;
                  List<ColumnDefinition> parents = new ArrayList<ColumnDefinition>();
                  ColumnDefinition d = defn.getParent();
                  while (d != null) {
                    parents.add(d);
                    d = d.getParent();
                  }
                  parent = values;
                  for (int i = parents.size() - 1; i >= 0; --i) {
                    Object o = parent.get(parents.get(i).getElementName());
                    if (o == null) {
                      parent = null;
                      break;
                    }
                    parent = (Map<String, Object>) o;
                  }
                  if (parent != null) {
                    Object o = parent.get(defn.getElementName());
                    if (o != null) {
                      if (dataType == ElementDataType.object
                          && type.getElementType().equals("mimeUri")) {
                        Map<String, Object> mimeuri = (Map<String, Object>) o;
                        String uriFragment = (String) mimeuri.get("uriFragment");
                        String contentType = (String) mimeuri.get("contentType");
                        File f = ODKFileUtils.getAsFile(appName, uriFragment);
                        if (f.equals(manifest)) {
                          throw new IllegalStateException("Unexpected collision with manifest.json");
                        }
                        freturn.addAttachmentFile(f, contentType);
                        parent.put(defn.getElementName(), f.getName());
                      } else {
                        throw new IllegalStateException("Unhandled transform case");
                      }
                    }
                  }
                }
              }

              // For JSON, we construct the model, then emit model +
              // meta + data
              HashMap<String, Object> wrapper = new HashMap<String, Object>();
              wrapper.put("tableId", tableId);
              wrapper.put("instanceId", instanceId);
              HashMap<String, Object> formDef = new HashMap<String, Object>();
              formDef.put("table_id", tableId);
              formDef.put("model", orderedDefns.getDataModel());
              wrapper.put("formDef", formDef);
              wrapper.put("data", values);
              wrapper.put("metadata", new HashMap<String, Object>());
              HashMap<String, Object> elem = (HashMap<String, Object>) wrapper.get("metadata");
              if (instanceName != null) {
                elem.put("instanceName", instanceName);
              }
              elem.put("saved", "COMPLETE");
              elem.put("timestamp", datestamp);

              b.append(ODKFileUtils.mapper.writeValueAsString(wrapper));

              // OK we have the document in the builder (b).
              String doc = b.toString();
              exportFile(doc, submissionXml, logger);
            }
            exportFile(freturn.serializeUriFragmentList(getContext()), manifest, logger);
            return ParcelFileDescriptor.open(manifest, ParcelFileDescriptor.MODE_READ_ONLY);

          }
        } finally {
          if (c != null && !c.isClosed()) {
            c.close();
            c = null;
          }
        }

      } catch (ParserConfigurationException e) {
        logger.printStackTrace(e);
      } catch (TransformerException e) {
        logger.printStackTrace(e);
      } catch (JsonParseException e) {
        logger.printStackTrace(e);
      } catch (JsonMappingException e) {
        logger.printStackTrace(e);
      } catch (IOException e) {
        logger.printStackTrace(e);
      }

    } finally {
      if ( db != null ) {
        try {
          // release the reference...
          // this does not necessarily close the db handle
          // or terminate any pending transaction
          db.releaseReference();
        } finally {
          // this will release the final reference and close the database
          OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
              dbHandleName);
        }
      }
    }
    return null;
  }

  /**
   * This method actually writes the JSON appName-relative manifest to disk.
   *
   * @param payload
   * @param outputFilePath
   * @param  logger
   * @return
   */
  private static boolean exportFile(String payload, File outputFilePath, WebLoggerIf logger) {
    // write xml file
    FileOutputStream os = null;
    OutputStreamWriter osw = null;
    try {
      os = new FileOutputStream(outputFilePath, false);
      osw = new OutputStreamWriter(os, CharEncoding.UTF_8);
      osw.write(payload);
      osw.flush();
      osw.close();
      return true;

    } catch (IOException e) {
      logger.e(t, "Error writing file");
      logger.printStackTrace(e);
      try {
        osw.close();
        os.close();
      } catch (IOException ex) {
        logger.printStackTrace(ex);
      }
      return false;
    }
  }

  @Override
  public int delete(@NonNull Uri uri, String selection, String[] selectionArgs) {
    return 0;
  }

  @Override
  public String getType(@NonNull Uri uri) {
    return null;
  }

  @Override
  public Uri insert(@NonNull Uri uri, ContentValues values) {
    return null;
  }

  @Override
  public Cursor query(@NonNull Uri uri, String[] projection, String selection, String[] selectionArgs,
      String sortOrder) {
    return null;
  }

  @Override
  public int update(@NonNull Uri uri, ContentValues values, String selection, String[] selectionArgs) {
    return 0;
  }

}
