/**
 * Copyright (C) 2015-2021 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.beeju.core;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.AUTO_CREATE_ALL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECTION_DRIVER;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECTION_USER_NAME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.PWD;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.SCHEMA_VERIFICATION;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;

public class BeejuCore {

  // "user" conflicts with USER db and the metastore_db can't be created.
  private static final String METASTORE_DB_USER = "db_user";
  private static final String METASTORE_DB_PASSWORD = "db_password";

  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private final String connectionURL;
  private final String driverClassName;
  private Path warehouseDir;
  
  private static Map<String, String> convertToMap(HiveConf hiveConf) {
    Map<String, String> converted = new HashMap<String, String>();
    Iterator<Entry<String, String>> iterator = hiveConf.iterator();
    while (iterator.hasNext()) {
      Entry<String, String> next = iterator.next();
      converted.put(next.getKey(), next.getValue());
    }
    return converted;
  }

  public BeejuCore() {
    this("test_database");
  }

  public BeejuCore(String databaseName) {
    this(databaseName, Collections.emptyMap());
  }
  
  public BeejuCore(String databaseName, HiveConf preConfiguration, HiveConf postConfiguration) {
    this(databaseName, convertToMap(preConfiguration), convertToMap(postConfiguration));
  }
  
  public BeejuCore(String databaseName, Map<String, String> preConfiguration) {
    this(databaseName, preConfiguration, Collections.emptyMap());
  }
  
  public BeejuCore(String databaseName, Map<String, String> preConfiguration, Map<String, String> postConfiguration) {
    checkNotNull(databaseName, "databaseName is required");
    this.databaseName = databaseName;
    configure(preConfiguration);

    driverClassName = EmbeddedDriver.class.getName();
    connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";

    System.setProperty("derby.system.home", "metastore_db_parent_" + UUID.randomUUID());

    // This should NOT be set as a system property too
    // conf.set(CONNECT_URL_KEY.getVarname(), connectionURL);
    setMetastoreAndSystemProperty(CONNECT_URL_KEY, connectionURL);

    setMetastoreAndSystemProperty(CONNECTION_DRIVER, driverClassName);
    setMetastoreAndSystemProperty(CONNECTION_USER_NAME, METASTORE_DB_USER);
    setMetastoreAndSystemProperty(PWD, METASTORE_DB_PASSWORD);

    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);

    setMetastoreAndSystemProperty(HMS_HANDLER_FORCE_RELOAD_CONF, "true");
    // Hive 2.x compatibility
    setMetastoreAndSystemProperty(AUTO_CREATE_ALL, "true");
    setMetastoreAndSystemProperty(SCHEMA_VERIFICATION, "false");

    // Used to prevent "Not authorized to make the get_current_notificationEventId call" errors
    setMetastoreAndSystemProperty(EVENT_DB_NOTIFICATION_API_AUTH, "false");

    // Used to prevent "Error polling for notification events" error
    conf.setTimeVar(HIVE_NOTFICATION_EVENT_POLL_INTERVAL, 0, TimeUnit.MILLISECONDS);

    // Has to be added to exclude failures related to the HiveMaterializedViewsRegistry
    conf.set(HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");
    System.setProperty(HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");

    // Override default port as some of our test environments claim it is in use.
    // This should not be equal to 0 as this would actually disable the WebUI and potentially
    // cause errors.
    conf.setInt(HIVE_SERVER2_WEBUI_PORT.varname, 20002); // ConfVars.HIVE_SERVER2_WEBUI_PORT

    // TODO: check if necessary or not
//    setMetastoreAndSystemProperty(HIVE_IN_TEST, "true");
//    setMetastoreAndSystemProperty(CONNECTION_POOLING_TYPE, "NONE");
//    setMetastoreAndSystemProperty(HIVE_SUPPORT_CONCURRENCY, "false");

//    setMetastoreAndSystemProperty(MULTITHREADED, "false");
//    setMetastoreAndSystemProperty(NON_TRANSACTIONAL_READ, "false");
//    setMetastoreAndSystemProperty(DATANUCLEUS_TRANSACTION_ISOLATION, "serializable");
    
    try {
      // overriding default derby log path to go to tmp
      String derbyLog = File.createTempFile("derby", ".log").getCanonicalPath();
      System.setProperty("derby.stream.error.file", derbyLog);

      //Creating temporary folder
      createWarehousePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    configure(postConfiguration);
  }

  private void setMetastoreAndSystemProperty(MetastoreConf.ConfVars key, String value) {
    conf.set(key.getVarname(), value);
    conf.set(key.getHiveName(), value);

    System.setProperty(key.getVarname(), value);
    System.setProperty(key.getHiveName(), value);
  }
  
  private void configure(Map<String, String> customConfiguration) {
    if (customConfiguration != null) {
      for (Map.Entry<String, String> entry : customConfiguration.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Initialise the warehouse path.
   *
   * @throws IOException If the initialisation fails.
   */
  private void createWarehousePath() throws IOException {
    warehouseDir = Files.createTempDirectory("beeju_test");
    setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());
  }

  void setHiveVar(HiveConf.ConfVars variable, String value) {
    conf.setVar(variable, value);
  }

  void setHiveConf(String variable, String value) {
    conf.set(variable, value);
  }

  void setHiveIntVar(HiveConf.ConfVars variable, int value) {
    conf.setIntVar(variable, value);
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    File tempFile = warehouseDir.toFile();
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(conf));
    String databaseFolder = new File(tempFile, databaseName).toURI().toString();
    try {
      client.createDatabase(new Database(databaseName, null, databaseFolder, null));
    } finally {
      client.close();
    }
  }

  /**
   * @return a copy of the {@link HiveConf} used to create the Hive Metastore database. This {@link HiveConf} should be
   * used by tests wishing to connect to the database.
   */
  public HiveConf conf() {
    return new HiveConf(conf);
  }

  /**
   * @return the name of the pre-created database.
   */
  public String databaseName() {
    return databaseName;
  }

  /**
   * @return the name of the JDBC driver class used to access the database.
   */
  public String driverClassName() {
    return driverClassName;
  }

  /**
   * @return the JDBC connection URL to the HSQLDB in-memory database.
   */
  public String connectionURL() {
    return connectionURL;
  }

  public Path tempDir() {
    return warehouseDir;
  }

  /**
   * Delete temporary files used during test
   */
  private void deleteTempDir() throws IOException {
    FileUtils.deleteDirectory(warehouseDir.toFile());
  }

  public void cleanUp() throws IOException {
    deleteTempDir();
  }

  public Path warehouseDir() {
    return warehouseDir;
  }

  /**
   * Creates a new HiveMetaStoreClient that can talk directly to the backed metastore database.
   * <p>
   * The invoker is responsible for closing the client.
   * </p>
   *
   * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
   */
  public HiveMetaStoreClient newClient() {
    try {
      return new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      throw new RuntimeException("Unable to create HiveMetaStoreClient", e);
    }
  }
}
