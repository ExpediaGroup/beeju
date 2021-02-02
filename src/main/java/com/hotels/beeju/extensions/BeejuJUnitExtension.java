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
package com.hotels.beeju.extensions;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.hotels.beeju.NoExitSecurityManager;
import com.hotels.beeju.core.BeejuCore;

/**
 * Base class for BeeJU JUnit Extensions that require a Hive Metastore database configuration pre-set.
 */
public abstract class BeejuJUnitExtension implements BeforeEachCallback, AfterEachCallback {

  protected BeejuCore core;

  public BeejuJUnitExtension(String databaseName, Map<String, String> configuration) {
    core = new BeejuCore(databaseName, configuration);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    NoExitSecurityManager securityManager = new NoExitSecurityManager();
    securityManager.setPolicy();
    System.setSecurityManager(securityManager);

    createDatabase(databaseName());
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    core.cleanUp();

    System.setSecurityManager(null);
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
   * @return {@link com.hotels.beeju.core.BeejuCore#connectionURL()}
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
