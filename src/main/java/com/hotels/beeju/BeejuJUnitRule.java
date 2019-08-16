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
import org.apache.thrift.TException;
import org.junit.rules.ExternalResource;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.beeju.core.BeejuCore;

/**
 * Base class for BeeJU JUnit Rules that require a Hive Metastore database configuration pre-set.
 */
abstract class BeejuJUnitRule extends ExternalResource {

  @VisibleForTesting
  public BeejuCore core;

  public BeejuJUnitRule(String databaseName, Map<String, String> configuration) {
    core = new BeejuCore(databaseName, configuration);
  }

  /**
   * This method can be overridden to provide additional initialisations.
   * </p>
   */
  protected void init() throws Exception {
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
    init();
    beforeTest();
    createDatabase(databaseName());
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
    core.deleteTempDir();
  }

  /**
   * @return {@link com.hotels.beeju.core.BeejuCore#driverClassName()}.
   */
  public String driverClassName() {
    return core.driverClassName();
  }

  /**
   * @return {@link com.hotels.beeju.core.BeejuCore#databaseName()}.
   */
  public String databaseName() {
    return core.databaseName();
  }

  /**
   * @return {@link com.hotels.beeju.core.BeejuCore#connectionURL()}.
   */
  public String connectionURL() {
    return core.connectionURL();
  }

  /**
   * @return {@link com.hotels.beeju.core.BeejuCore#conf()}.
   */
  public HiveConf conf() {
    return core.conf();
  }

  /**
   * @return {@link com.hotels.beeju.core.BeejuCore#newClient()}.
   */
  public HiveMetaStoreClient newClient() {
    return core.newClient();
  }

  /**
   * @return Root of temporary directory
   */
  public File tempDir() {
    return core.tempDir().toFile();
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    core.createDatabase(databaseName);
  }
}
