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

import com.hotels.beeju.core.ThriftHiveMetaStoreCore;

public class ThriftHiveMetaStoreJUnitExtension extends HiveMetaStoreJUnitExtension {

  private ThriftHiveMetaStoreCore thriftHiveMetaStoreCore;

  public ThriftHiveMetaStoreJUnitExtension() {
    this("test_database");
  }

  public ThriftHiveMetaStoreJUnitExtension(String databaseName){
    this(databaseName, null);
  }

  public ThriftHiveMetaStoreJUnitExtension(String databaseName, Map<String, String> configuration){
    super(databaseName,configuration);
    thriftHiveMetaStoreCore = new ThriftHiveMetaStoreCore(core);
  }

  @Override
  public void beforeTest() {
    try {
      thriftHiveMetaStoreCore.initialise();
      super.beforeTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void afterTest() {
    thriftHiveMetaStoreCore.shutdown();
    super.afterTest();
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
}
