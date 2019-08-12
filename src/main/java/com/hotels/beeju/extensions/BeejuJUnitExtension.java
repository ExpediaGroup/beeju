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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.extension.*;

import com.hotels.beeju.core.BeejuCore;

/**
 * Might not need to use this
 */
abstract public class BeejuJUnitExtension implements BeforeEachCallback, AfterEachCallback {

  public BeejuCore core;
  private Path tempDirectory;

  public BeejuJUnitExtension(String databaseName, Map<String, String> configuration) {
    core = new BeejuCore(databaseName,configuration);
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
    tempDirectory = Files.createTempDirectory("root");
    core.setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, tempDirectory.toString());
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
  public void beforeEach(ExtensionContext context) throws TException{
    try {
      init();
      beforeTest();
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
    core.createDatabase(core.databaseName(), tempDirectory.toFile());
  }

  /**
   * Implement method to release any resources used by the rule.
   * <p>
   * This method is called before the warehouse directory is deleted.
   * </p>
   */
  protected abstract void afterTest();

  @Override
  public void afterEach(ExtensionContext context) {
    afterTest();
    try {
      FileUtils.deleteDirectory(tempDirectory.toFile());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * @return the name of the JDBC driver class used to access the database.
   */
  public String driverClassName() {
    return core.driverClassName();
  }

  /**
   * @return the name of the pre-created database.
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
   * @return a copy of the {@link HiveConf} used to create the Hive Metastore database. This {@link HiveConf} should be
   *         used by tests wishing to connect to the database.
   */
  public HiveConf conf() {
    return core.conf();
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName) throws TException {
    File tempFolder = tempDirectory.toFile();
    core.createDatabase(databaseName, tempFolder);
  }

  public HiveMetaStoreClient newClient (){
    return core.newClient();
  }

  public File getTempDirectory(){
    return tempDirectory.toFile();
  }
}
