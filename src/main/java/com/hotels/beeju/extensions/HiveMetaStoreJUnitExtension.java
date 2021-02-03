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

import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.hotels.beeju.core.HiveMetaStoreCore;

/**
 * A JUnit Extension that creates a Hive Metastore backed by an in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 */
public class HiveMetaStoreJUnitExtension extends BeejuJUnitExtension {

  private final HiveMetaStoreCore hiveMetaStoreCore;

  /**
   * Create a Hive Metastore with a pre-created database "test_database".
   */
  public HiveMetaStoreJUnitExtension() {
    this("test_database");
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public HiveMetaStoreJUnitExtension(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  public HiveMetaStoreJUnitExtension(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
    hiveMetaStoreCore = new HiveMetaStoreCore(core);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    super.beforeEach(context);
    hiveMetaStoreCore.initialise();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    hiveMetaStoreCore.shutdown();
    super.afterEach(context);
  }

  /**
   * @return {@link com.hotels.beeju.core.HiveMetaStoreCore#client()}.
   */
  public HiveMetaStoreClient client() {
    return hiveMetaStoreCore.client();
  }
}
