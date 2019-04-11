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

import java.net.ServerSocket;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.service.Service.STATE;
import org.apache.hive.service.server.HiveServer2;

import com.hotels.beeju.hiveserver2.RelaxedSQLStdHiveAuthorizerFactory;

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

  private String jdbcConnectionUrl;
  private HiveServer2 hiveServer2;
  private int port;

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
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
  }

  @Override
  protected void beforeTest() throws Throwable {
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, RelaxedSQLStdHiveAuthorizerFactory.class.getName());
    hiveServer2 = new HiveServer2();
    hiveServer2.init(conf);
    hiveServer2.start();
    waitForHiveServer2StartUp();

    jdbcConnectionUrl = "jdbc:hive2://localhost:" + port + "/" + databaseName();
  }

  private void waitForHiveServer2StartUp() throws InterruptedException {
    int retries = 0;
    int maxRetries = 5;
    while (hiveServer2.getServiceState() != STATE.STARTED && retries < maxRetries) {
      Thread.sleep(1000);
      retries++;
    }
    if (retries >= maxRetries) {
      throw new RuntimeException("HiveServer2 did not start in a reasonable time");
    }
  }

  @Override
  protected void afterTest() {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
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
    return jdbcConnectionUrl;
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
      return new HiveMetaStoreClient(conf());
    } catch (MetaException e) {
      throw new RuntimeException("Unable to create HiveMetaStoreClient", e);
    }
  }

}
