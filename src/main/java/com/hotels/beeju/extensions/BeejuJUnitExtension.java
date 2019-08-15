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
package com.hotels.beeju.extensions;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.hotels.beeju.core.BeejuCore;

abstract public class BeejuJUnitExtension implements BeforeEachCallback, AfterEachCallback {

  public BeejuCore core;

  public BeejuJUnitExtension(String databaseName, Map<String, String> configuration) {
    core = new BeejuCore(databaseName, configuration);
  }

  /**
   * Initialise the warehouse path.
   * <p>
   * This method can be overridden to provide additional initialisations.
   * </p>
   */
  protected void init() throws Exception {
    core.init();
  }

  /**
   * Implement this method to prepare the extension for a new test.
   * <p>
   * This is called after the extension is initialised.
   * </p>
   *
   * @throws Exception If the extension cannot be prepared for a new test.
   */
  protected abstract void beforeTest() throws Exception;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
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
  protected abstract void afterTest() throws Exception;

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
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
   * @return @see com.hotels.beeju.core.BeejuCore#connectionURL()
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
   * See {@link com.hotels.beeju.core.BeejuCore#createDatabase(String)}
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    core.createDatabase(databaseName);
  }

  /**
   * @return Root temporary directory as a file.
   */
  public File getTempDirectory() {
    return core.tempDir().toFile();
  }
}
