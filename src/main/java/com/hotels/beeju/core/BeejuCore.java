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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.*;

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

import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeejuCore {
  
  private static final Logger log = LoggerFactory.getLogger(BeejuCore.class);

  // "user" conflicts with USER db and the metastore_db can't be created.
  private static final String METASTORE_DB_USER = "db_user";
  private static final String METASTORE_DB_PASSWORD = "db_password";

  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private String connectionURL;
  private final String driverClassName;
  private Path tempDir;
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
    
    //connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() ;//+ ";create=true";
    
    
    configureMiscHiveSettings(conf);

    configureMetaStore(conf);

    configureMrExecutionEngine(conf);

    configureTezExecutionEngine(conf);

    configureJavaSecurityRealm(conf);

    configureSupportConcurrency(conf);

    try {
      tempDir = Files.createTempDirectory("beeju_test");
      System.err.println("XXX CREATED TMP DIR " + tempDir + " FOR DB " + databaseName);
      configureFileSystem(tempDir, conf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    configureAssertionStatus(conf);
    
    
    setMetastoreAndSystemProperty(EVENT_DB_NOTIFICATION_API_AUTH, "false");
    setMetastoreAndSystemProperty(CONNECTION_USER_NAME, METASTORE_DB_USER);
    setMetastoreAndSystemProperty(PWD, METASTORE_DB_PASSWORD);

    
/*
    // This should NOT be set as a system property too
    conf.set(CONNECT_URL_KEY.getVarname(), connectionURL);

    setMetastoreAndSystemProperty(CONNECTION_DRIVER, driverClassName);
    setMetastoreAndSystemProperty(CONNECTION_USER_NAME, METASTORE_DB_USER);
    setMetastoreAndSystemProperty(PWD, METASTORE_DB_PASSWORD);

    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
    conf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

    setMetastoreAndSystemProperty(HMS_HANDLER_FORCE_RELOAD_CONF, "true");
    // Hive 2.x compatibility
    setMetastoreAndSystemProperty(AUTO_CREATE_ALL, "true");
    conf.set("datanucleus.schema.autoCreateTables", "true");
    setMetastoreAndSystemProperty(SCHEMA_VERIFICATION, "false");

    // Used to prevent "Not authorized to make the get_current_notificationEventId call" errors
    setMetastoreAndSystemProperty(EVENT_DB_NOTIFICATION_API_AUTH, "false");

    // TODO: check if necessary or not
    //NON_TRANSACTIONAL_READ -> true
//   setMetastoreAndSystemProperty(HIVE_IN_TEST, "true");
//   setMetastoreAndSystemProperty(HIVE_IN_TEZ_TEST, "true");
   setMetastoreAndSystemProperty(CONNECTION_POOLING_TYPE, "NONE");
   setMetastoreAndSystemProperty(org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, "false");
//   setMetastoreAndSystemProperty(COMPACTOR_INITIATOR_ON, "true");
//   setMetastoreAndSystemProperty(COMPACTOR_WORKER_THREADS, "2");
   setMetastoreAndSystemProperty( TXN_TIMEOUT, "5");
//   setMetastoreAndSystemProperty( TIMEDOUT_TXN_REAPER_START, "5");
//   setMetastoreAndSystemProperty( TIMEDOUT_TXN_REAPER_INTERVAL, "5");
   //setMetastoreAndSystemProperty(HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");

    // override default port as some of our test environments claim it is in use.
    conf.setInt("hive.server2.webui.port", 0); // ConfVars.HIVE_SERVER2_WEBUI_PORT
    //conf.setBoolean("hive.txn.strict.locking.mode", false); 
    
    try {
      // overriding default derby log path to go to tmp
      String derbyLog = File.createTempFile("derby", ".log").getCanonicalPath();
      System.setProperty("derby.stream.error.file", derbyLog);

      //Creating temporary folder
      createWarehousePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }*/
    
    configure(postConfiguration);
  }
  
  
  
  protected void configureMiscHiveSettings(HiveConf hiveConf) {
    hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

    // Turn of dependency to calcite library
    hiveConf.setBoolVar(HIVE_CBO_ENABLED, false);

    // Disable to get rid of clean up exception when stopping the Session.
    hiveConf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

    hiveConf.setVar(HADOOPBIN, "NO_BIN!");
  }

//  protected void overrideHiveConf(HiveConf hiveConf) {
//    for (Map.Entry<String, String> hiveConfEntry : hiveRunnerConfig.getHiveConfSystemOverride().entrySet()) {
//      hiveConf.set(hiveConfEntry.getKey(), hiveConfEntry.getValue());
//    }
//  }

  protected void configureMrExecutionEngine(HiveConf conf) {

    /*
     * Switch off all optimizers otherwise we didn't manage to contain the map reduction within this JVM.
     */
    conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
    conf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
    conf.setBoolVar(HIVEOPTINDEXFILTER, false);
    conf.setBoolVar(HIVECONVERTJOIN, false);
    conf.setBoolVar(HIVESKEWJOIN, false);

    // Defaults to a 1000 millis sleep in. We can speed up the tests a bit by setting this to 1 millis instead.
    // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
    conf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

    conf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
  }

  protected void configureTezExecutionEngine(HiveConf conf) {
    /*
     * Tez local mode settings
     */
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

    /*
     * Set to be able to run tests offline
     */
    conf.set(TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK, "true");

    /*
     * General attempts to strip of unnecessary functionality to speed up test execution and increase stability
     */
    conf.set(TezConfiguration.TEZ_AM_USE_CONCURRENT_DISPATCHER, "false");
    conf.set(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, "false");
    conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
    conf.set(TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX, "1");
    conf.set(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, "false");
    conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
    conf.set(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, "false");
  }

  protected void configureJavaSecurityRealm(HiveConf hiveConf) {
    // These three properties gets rid of: 'Unable to load realm info from SCDynamicStore'
    // which seems to have a timeout of about 5 secs.
    System.setProperty("java.security.krb5.realm", "");
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.conf", "/dev/null");
  }

  protected void configureAssertionStatus(HiveConf conf) {
    ClassLoader
        .getSystemClassLoader()
        .setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector", false);
  }

  protected void configureSupportConcurrency(HiveConf conf) {
    conf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
  }

  protected void configureMetaStore(HiveConf conf) {
    configureDerbyLog();

    String jdbcDriver = org.apache.derby.jdbc.EmbeddedDriver.class.getName();
    try {
      Class.forName(jdbcDriver);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Set the Hive Metastore DB driver
    connectionURL = "jdbc:derby:memory:" + UUID.randomUUID().toString();
    setMetastoreProperty("datanucleus.schema.autoCreateAll", "true");
    setMetastoreProperty("datanucleus.schema.autoCreateTables", "true");
    setMetastoreProperty("hive.metastore.schema.verification", "false");
    setMetastoreProperty("metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");

    setMetastoreProperty("datanucleus.connectiondrivername", jdbcDriver);
    setMetastoreProperty("javax.jdo.option.ConnectionDriverName", jdbcDriver);

    // No pooling needed. This will save us a lot of threads
    setMetastoreProperty("datanucleus.connectionPoolingType", "None");

    setMetastoreProperty(METASTORE_VALIDATE_CONSTRAINTS.varname, "true");
    setMetastoreProperty(METASTORE_VALIDATE_COLUMNS.varname, "true");
    setMetastoreProperty(METASTORE_VALIDATE_TABLES.varname, "true");
  }

  private void configureDerbyLog() {
    // overriding default derby log path to not go to root of project
    File derbyLogFile;
    try {
      derbyLogFile = File.createTempFile("derby", ".log");
      log.debug("Derby set to log to " + derbyLogFile.getAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating temporary derby log file", e);
    }
    System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
  }

  protected void configureFileSystem(Path basedir, HiveConf conf) throws IOException {
    setMetastoreProperty(METASTORECONNECTURLKEY.varname, connectionURL + ";create=true");

    createAndSetFolderProperty(METASTOREWAREHOUSE, "warehouse", conf, basedir);
    warehouseDir = new File(conf.getVar(METASTOREWAREHOUSE)).toPath();
    createAndSetFolderProperty(SCRATCHDIR, "scratchdir", conf, basedir);
    createAndSetFolderProperty(LOCALSCRATCHDIR, "localscratchdir", conf, basedir);
    createAndSetFolderProperty(HIVEHISTORYFILELOC, "tmp", conf, basedir);

    createAndSetFolderProperty("hadoop.tmp.dir", "hadooptmp", conf, basedir);
    createAndSetFolderProperty("test.log.dir", "logs", conf, basedir);

    /*
     * Tez specific configurations below
     */
    /*
     * Tez will upload a hive-exec.jar to this location. It looks like it will do this only once per test suite so it
     * makes sense to keep this in a central location rather than in the tmp dir of each test.
     */
    File installation_dir = newFolder(basedir, "tez_installation_dir").toFile();

    conf.setVar(HiveConf.ConfVars.HIVE_JAR_DIRECTORY, installation_dir.getAbsolutePath());
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, installation_dir.getAbsolutePath());
  }

  Path newFolder(Path basedir, String folder) throws IOException {
    Path newFolder = Files.createTempDirectory(basedir, folder);
    FileUtil.setPermission(newFolder.toFile(), FsPermission.getDirDefault());
    return newFolder;
  }
  
  protected final void createAndSetFolderProperty(HiveConf.ConfVars var, String folder, HiveConf conf, Path basedir)
      throws IOException {
      setMetastoreProperty(var.varname, newFolder(basedir, folder).toAbsolutePath().toString());
    }

    protected final void createAndSetFolderProperty(String key, String folder, HiveConf conf, Path basedir)
      throws IOException {
      setMetastoreProperty(key, newFolder(basedir, folder).toAbsolutePath().toString());
    }

    protected final void setMetastoreProperty(String key, String value) {
      conf.set(key, value);
      System.setProperty(key, value);
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
    tempDir = Files.createTempDirectory("beeju_test");
    setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, tempDir.toString());
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
    File tempFile = tempDir.toFile();
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(conf)); //TODO: why not use our own newClient() here?
    String databaseFolder = new File(tempFile, databaseName).toURI().toString();
    System.err.println("XXX DB FOLDER for : " + databaseName + " IS " + databaseFolder);
    try { //TODO: try with resources for client?
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
    return tempDir;
  }
  
  public Path warehouseDir() {
    return warehouseDir;
  }

  /**
   * Delete temporary files used during test
   */
  private void deleteTempDir() throws IOException {
    FileUtils.deleteDirectory(tempDir.toFile());
  }

  public void cleanUp() throws IOException {
    deleteTempDir();
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
