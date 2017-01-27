/**
 * Copyright (C) 2015-2017 Expedia Inc.
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
package com.hotels.beeju;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.google.common.annotations.VisibleForTesting;

/**
 * A JUnit {@link Rule} that creates a Hive Metastore backed by an HSQLDB in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * <p>
 * To allow querying of the memory database, add the following to your {@code @Before} method. This will open a Swing
 * SQL query window for the duration of the test. Remember to add a break point to your test.
 *
 * <pre>
 * &#064;Rule
 * public HiveMetaStoreJUnitRule hive = new HiveMetaStoreJUnitRule();
 *
 * &#064;Override
 * protected void before() throws Throwable {
 *   org.hsqldb.util.DatabaseManagerSwing.main(new String[] { &quot;--url&quot;, hive.connectionURL(), &quot;--user&quot;,
 *       HiveMetaStoreJUnitRule.HSQLDB_USER, &quot;--password&quot;, HiveMetaStoreJUnitRule.HSQLDB_PASSWORD, &quot;--noexit&quot; });
 *
 * }
 * </pre>
 */
public class HiveMetaStoreJUnitRule extends ExternalResource {

  public static final String HSQLDB_USER = "user";
  public static final String HSQLDB_PASSWORD = "password";

  @VisibleForTesting
  final TemporaryFolder temporaryFolder = new TemporaryFolder();
  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private final String connectionURL;
  private final String driverClassName;
  private HiveMetaStoreClient client;
  private File metastoreLocation;

  /**
   * Create a Hive Metastore with a pre-created database "test_database".
   */
  public HiveMetaStoreJUnitRule() {
    this("test_database");
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public HiveMetaStoreJUnitRule(String databaseName) {
    this.databaseName = databaseName;
    driverClassName = JDBCDriver.class.getName();
    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
    connectionURL = "jdbc:hsqldb:mem:" + UUID.randomUUID();
    conf.setVar(ConfVars.METASTORECONNECTURLKEY, connectionURL);
    conf.setVar(ConfVars.METASTORE_CONNECTION_DRIVER, driverClassName);
    conf.setVar(ConfVars.METASTORE_CONNECTION_USER_NAME, HSQLDB_USER);
    conf.setVar(ConfVars.METASTOREPWD, HSQLDB_PASSWORD);
    conf.setBoolVar(ConfVars.HMSHANDLERFORCERELOADCONF, true);
    // Hive 2.x compatibility
    conf.setBoolean("datanucleus.schema.autoCreateAll", true);
  }

  private void postInit() throws IOException {
    metastoreLocation = temporaryFolder.newFolder("metastore");
    conf.setVar(ConfVars.METASTOREWAREHOUSE, metastoreLocation.getAbsolutePath());
  }

  @Override
  protected void before() throws Throwable {
    temporaryFolder.create();
    postInit();

    final HiveConf hiveConf = new HiveConf(conf, HiveMetaStoreClient.class);
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    try {
      client = singleThreadExecutor.submit(new Callable<HiveMetaStoreClient>() {
        @Override
        public HiveMetaStoreClient call() throws Exception {
          return new HiveMetaStoreClient(hiveConf);
        }
      }).get();
    } finally {
      singleThreadExecutor.shutdown();
    }

    if (databaseName != null) {
      createDatabase(databaseName);
    }
  }

  @Override
  protected void after() {
    client.close();
    temporaryFolder.delete();
  }

  /**
   * @return the name of the JDBC driver class used to access the database.
   */
  public String driverClassName() {
    return driverClassName;
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
   * @return the JDBC connection URL to the HSQLDB in-memory database.
   */
  public String connectionURL() {
    return connectionURL;
  }

  /**
   * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
   */
  public HiveMetaStoreClient client() {
    return client;
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    String databaseFolder = new File(temporaryFolder.getRoot(), databaseName).toURI().toString();
    client.createDatabase(new Database(databaseName, null, databaseFolder, null));
  }

}
