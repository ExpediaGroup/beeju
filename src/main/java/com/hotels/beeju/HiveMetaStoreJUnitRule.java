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

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.hotels.beeju.core.HiveMetaStoreCore;

/**
 * A JUnit {@link org.junit.Rule} that creates a Hive Metastore backed by an in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 */
public class HiveMetaStoreJUnitRule extends BeejuJUnitRule {

  private final HiveMetaStoreCore hiveMetaStoreCore = new HiveMetaStoreCore(core);

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
    this(databaseName, null);
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  public HiveMetaStoreJUnitRule(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    hiveMetaStoreCore.initialise();
  }

  @Override
  protected void after() {
    hiveMetaStoreCore.shutdown();
    super.after();
  }

  /**
   * @return the {@link com.hotels.beeju.core.HiveMetaStoreCore#client()}.
   */
  public HiveMetaStoreClient client() {
    return hiveMetaStoreCore.client();
  }
}
