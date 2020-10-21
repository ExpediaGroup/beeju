/**
 * Copyright (C) 2015-2020 Expedia, Inc.
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

import com.hotels.beeju.core.ThriftHiveMetaStoreCore;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * A JUnit Rule that creates a Hive Metastore Thrift service backed by a Hive Metastore using an HSQLDB in-memory
 * database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 */
public class ThriftHiveMetaStoreJUnitRule extends HiveMetaStoreJUnitRule {

  private ThriftHiveMetaStoreCore thriftHiveMetaStoreCore = new ThriftHiveMetaStoreCore(core);

  /**
   * Create a Thrift Hive Metastore service with a pre-created database "test_database".
   */
  public ThriftHiveMetaStoreJUnitRule() {
    this("test_database");
  }

  /**
   * Create a Thrift Hive Metastore service with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public ThriftHiveMetaStoreJUnitRule(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a Thrift Hive Metastore service with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  public ThriftHiveMetaStoreJUnitRule(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
  }

  @Override
  protected void before() throws Throwable {
    thriftHiveMetaStoreCore.initialise();
    super.before();
  }

  @Override
  protected void after() {
    thriftHiveMetaStoreCore.shutdown();
    super.after();
  }

  /**
   * @return {@link com.hotels.beeju.core.ThriftHiveMetaStoreCore#getThriftConnectionUri()}.
   */
  public String getThriftConnectionUri() {
    return thriftHiveMetaStoreCore.getThriftConnectionUri();
  }

  /**
   * @return {@link com.hotels.beeju.core.ThriftHiveMetaStoreCore#getThriftPort()}
   */
  public int getThriftPort() {
    return thriftHiveMetaStoreCore.getThriftPort();
  }
  
  /**
   * @param thriftPort The Port to use for the Thrift Hive metastore, if not set then a port number will automatically be allocated.
   */
  public void setThriftPort(int thriftPort) {
    thriftHiveMetaStoreCore.setThriftPort(thriftPort);
  }

}
