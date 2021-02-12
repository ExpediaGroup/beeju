/**
 * Copyright (C) 2015-2021 Expedia, Inc. and Klarna AB.
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class contains some code sourced from and inspired by HiveRunner, specifically 
// https://github.com/klarna/HiveRunner/blob/fb00a98f37abdb779547c1c98ef6fbe54d373e0c/src/main/java/com/klarna/hiverunner/StandaloneHiveServerContext.java
public class BeejuCore {

  private static final Logger log = LoggerFactory.getLogger(BeejuCore.class);

  // "user" conflicts with USER db and the metastore_db can't be created.
  private static final String METASTORE_DB_USER = "db_user";
  private static final String METASTORE_DB_PASSWORD = "db_password";

  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private String connectionURL;
  private String driverClassName;
  private Path warehouseDir;
  private Path derbyHome;
  private Path baseDir;

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

    configureFolders();
    
    configureMetastore();
    
    configureMisc();

    configure(postConfiguration);
  }

  private void configureMisc() {
    // override default port as some of our test environments claim it is in use.
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, 0);
    
    conf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    
    // Disable to get rid of clean up exception when stopping the Session.
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

    // Used to prevent "Not authorized to make the get_current_notificationEventId call" errors
    setMetastoreAndSystemProperty(MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, "false");

    // Used to prevent "Error polling for notification events" error
    conf.setTimeVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, 0, TimeUnit.MILLISECONDS);

    // Has to be added to exclude failures related to the HiveMaterializedViewsRegistry
    conf.set(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");
    System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");
  }

  private void setMetastoreAndSystemProperty(MetastoreConf.ConfVars key, String value) {
    conf.set(key.getVarname(), value);
    conf.set(key.getHiveName(), value);

    System.setProperty(key.getVarname(), value);
    System.setProperty(key.getHiveName(), value);
  }

  private void configureFolders() {
    try {
      baseDir = Files.createTempDirectory("beeju-basedir-");
      createAndSetFolderProperty(HiveConf.ConfVars.SCRATCHDIR, "scratchdir");
      createAndSetFolderProperty(HiveConf.ConfVars.LOCALSCRATCHDIR, "localscratchdir");
      createAndSetFolderProperty(HiveConf.ConfVars.HIVEHISTORYFILELOC, "hive-history");
      
      createDerbyPaths();
      createWarehousePath();
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating temporary folders", e);
    }
  }
  
  private void configureMetastore() {
    driverClassName = EmbeddedDriver.class.getName();
    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
    connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";

//    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, connectionURL);
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, driverClassName);
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, METASTORE_DB_USER);
//    conf.setVar(HiveConf.ConfVars.METASTOREPWD, METASTORE_DB_PASSWORD);

    setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECT_URL_KEY, connectionURL);
    setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECTION_DRIVER, driverClassName);
    setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECTION_USER_NAME, METASTORE_DB_USER);
    setMetastoreAndSystemProperty(MetastoreConf.ConfVars.PWD, METASTORE_DB_PASSWORD);

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "NONE");
    conf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, true);

    // Hive 2.x compatibility
    conf.setBoolean("datanucleus.schema.autoCreateAll", true);
    conf.setBoolean("hive.metastore.schema.verification", false);

    System.setProperty("datanucleus.schema.autoCreateAll", "true");
    System.setProperty("hive.metastore.schema.verification", "false");
  }

  private void createAndSetFolderProperty(HiveConf.ConfVars var, String childFolderName) throws IOException {
    String folderPath = newFolder(baseDir, childFolderName).toAbsolutePath().toString();
    conf.setVar(var, folderPath);
  }

  private Path newFolder(Path basedir, String folder) throws IOException {
    Path newFolder = Files.createTempDirectory(basedir, folder);
    FileUtil.setPermission(newFolder.toFile(), FsPermission.getDirDefault());
    return newFolder;
  }

  private void createDerbyPaths() throws IOException {
    derbyHome = Files.createTempDirectory(baseDir, "derby-home-");
    System.setProperty("derby.system.home", derbyHome.toString());

    // overriding default derby log path to go to tmp
    String derbyLog = Files.createTempFile(baseDir, "derby", ".log").toString();
    System.setProperty("derby.stream.error.file", derbyLog);
  }

  private void createWarehousePath() throws IOException {
    warehouseDir = Files.createTempDirectory(baseDir, "hive-warehouse-");
    setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());
  }

  public void cleanUp() {
    deleteDirectory(baseDir);
  }

  private void deleteDirectory(Path path) {
    try {
      FileUtils.deleteDirectory(path.toFile());
    } catch (IOException e) {
      log.warn("Error cleaning up " + path, e);
    }
  }

  private void configure(Map<String, String> customConfiguration) {
    if (customConfiguration != null) {
      for (Map.Entry<String, String> entry : customConfiguration.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
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
    String databaseFolder = new File(tempFile, databaseName).toURI().toString();
    HiveMetaStoreClient client = newClient();
    try {
      client.createDatabase(new Database(databaseName, null, databaseFolder, null));
    } finally {
      client.close();
    }
  }

  /**
   * @return a copy of the {@link HiveConf} used to create the Hive Metastore database. This {@link HiveConf} should be
   *         used by tests wishing to connect to the database.
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
    return baseDir;
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
