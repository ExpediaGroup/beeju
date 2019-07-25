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
package com.hotels.beeju;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.beeju.core.BeejuCore;

/**
 * Base class for BeeJU JUnit Rules that require a Hive Metastore database configuration pre-set.
 */
abstract class BeejuJUnitRule extends ExternalResource {

  @VisibleForTesting
  final TemporaryFolder temporaryFolder = new TemporaryFolder();
  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private File metastoreLocation;
  private BeejuCore core;

  public BeejuJUnitRule(String databaseName, Map<String, String> configuration) {
    this.databaseName = databaseName;
    core = new BeejuCore(conf, databaseName,configuration);
  }

  /**
   * Initialise the warehouse path.
   * <p>
   * This method can be overridden to provide additional initialisations.
   * </p>
   *
   * @throws Throwable If the initialisation fails.
   */
  protected void init() throws Throwable {
    metastoreLocation = temporaryFolder.newFolder("metastore");
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, metastoreLocation.getAbsolutePath());
  }

  /**
   * Implement this method to prepare the rule for a new test.
   * <p>
   * This is called after the rule is initialised.
   * </p>
   *
   * @throws Throwable If the rule cannot be prepared for a new test.
   */
  protected abstract void beforeTest() throws Throwable;

  @Override
  protected void before() throws Throwable {
    temporaryFolder.create();
    init();
    beforeTest();
    createDatabase(databaseName);
  }

  /**
   * Implement method to release any resources used by the rule.
   * <p>
   * This method is called before the warehouse directory is deleted.
   * </p>
   */
  protected abstract void afterTest();

  @Override
  protected void after() {
    afterTest();
    temporaryFolder.delete();
  }

  /**
   * @return the name of the JDBC driver class used to access the database.
   */
  public String driverClassName() {
    return core.driverClassName();
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
    return core.connectionURL();
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf());
    String databaseFolder = new File(temporaryFolder.getRoot(), databaseName).toURI().toString();
    try {
      client.createDatabase(new Database(databaseName, null, databaseFolder, null));
    } finally {
      client.close();
    }
  }

}
