/**
 * Copyright (C) 2015-2019 Expedia, Inc.
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

public class BeejuCore {

  // "user" conflicts with USER db and the metastore_db can't be created.
  private static final String METASTORE_DB_USER = "db_user";
  private static final String METASTORE_DB_PASSWORD = "db_password";

  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private final String connectionURL;
  private final String driverClassName;
  private Path tempDir;

  public BeejuCore() {
    this("test_database");
  }

  public BeejuCore(String databaseName) {
    this(databaseName, null);
  }

  public BeejuCore(String databaseName, Map<String, String> configuration) {
    checkNotNull(databaseName, "databaseName is required");
    this.databaseName = databaseName;

    if (configuration != null && !configuration.isEmpty()) {
      for (Map.Entry<String, String> entry : configuration.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    driverClassName = EmbeddedDriver.class.getName();
    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
    connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";
    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, connectionURL);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, driverClassName);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, METASTORE_DB_USER);
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, METASTORE_DB_PASSWORD);
    conf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, true);
    // Hive 2.x compatibility
    conf.setBoolean("datanucleus.schema.autoCreateAll", true);
    conf.setBoolean("hive.metastore.schema.verification", false);
    // override default port as some of our test environments claim it is in use.
    conf.setInt("hive.server2.webui.port", 0); // ConfVars.HIVE_SERVER2_WEBUI_PORT
    try {
      // overriding default derby log path to go to tmp
      String derbyLog = File.createTempFile("derby", ".log").getCanonicalPath();
      System.setProperty("derby.stream.error.file", derbyLog);

      //Creating temporary folder
      createWarehousePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    return tempDir;
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
