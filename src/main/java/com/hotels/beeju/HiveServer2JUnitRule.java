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

import java.util.Map;

import com.hotels.beeju.core.HiveServer2Core;
import org.apache.hive.jdbc.HiveDriver;

/**
 * A JUnit Rule that creates a HiveServer2 service and Thrift Metastore service backed by a Hive Metastore using an
 * HSQLDB in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 * <p>
 * Note that this class is meant to be used for DDL operations only. Any attempt to run DML operations may result in
 * errors.
 * </p>
 */
public class HiveServer2JUnitRule extends BeejuJUnitRule {

  private HiveServer2Core hiveServer2Core = new HiveServer2Core(core);

  /**
   * Create a HiveServer2 service with a pre-created database "test_database".
   */
  public HiveServer2JUnitRule() {
    this("test_database");
  }

  /**
   * Create a HiveServer2 service with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public HiveServer2JUnitRule(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a HiveServer2 service with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  public HiveServer2JUnitRule(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
  }

  @Override
  protected void init() throws Throwable {
    super.init();
    hiveServer2Core.startServerSocket();
  }

  @Override
  protected void beforeTest() throws Throwable {
    hiveServer2Core.before();
  }

  @Override
  protected void afterTest() {
    hiveServer2Core.after();
  }

  /**
   * @return the name of the Hive JDBC driver class used to access the database.
   */
  @Override
  public String driverClassName() {
    return HiveDriver.class.getName();
  }

  /**
   * @return the JDBC connection URL to the HiveServer2 service.
   */
  @Override
  public String connectionURL() {
    return hiveServer2Core.getJdbcConnectionUrl();
  }


}
