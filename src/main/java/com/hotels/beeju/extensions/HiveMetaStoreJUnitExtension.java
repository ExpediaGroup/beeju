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

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;

import com.hotels.beeju.core.HiveMetaStoreCore;

public class HiveMetaStoreJUnitExtension extends BeejuJUnitExtension implements BeforeEachCallback, AfterEachCallback {

  private HiveMetaStoreCore hiveMetaStoreCore;

  HiveMetaStoreJUnitExtension() {
    this("test_database");
  }

  HiveMetaStoreJUnitExtension(String databaseName){
    this(databaseName, null);
  }

  HiveMetaStoreJUnitExtension(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
    hiveMetaStoreCore = new HiveMetaStoreCore(core);
  }

  @Override
  public void beforeTest() throws InterruptedException, ExecutionException {
    hiveMetaStoreCore.initialise();
  }

  @Override
  public void afterTest() {
    hiveMetaStoreCore.shutdown();
  }

  /**
   * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
   */
  public HiveMetaStoreClient client() {
    return hiveMetaStoreCore.client();
  }
}
