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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ThriftHiveMetaStoreJUnitExtensionTest {

  @RegisterExtension
  ThriftHiveMetaStoreJUnitExtension defaultDbExtension = new ThriftHiveMetaStoreJUnitExtension();

  @RegisterExtension
  ThriftHiveMetaStoreJUnitExtension customDbExtension = new ThriftHiveMetaStoreJUnitExtension("my_test_database");

  @RegisterExtension
  ThriftHiveMetaStoreJUnitExtension customPropertiesExtension = new ThriftHiveMetaStoreJUnitExtension(
      "custom_props_database", customConfProperties());

  private void assertExtensionInitialised(ThriftHiveMetaStoreJUnitExtension hive) throws Exception {
    String databaseName = hive.databaseName();

    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.getWarehouseDirectory(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));

    assertThat(hive.getThriftConnectionUri(), is("thrift://localhost:" + hive.getThriftPort()));
    HiveConf conf = new HiveConf(hive.conf());
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, hive.getThriftConnectionUri());
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      List<String> databases = client.getAllDatabases();
      assertThat(databases.size(), is(2));
      assertThat(databases.get(0), is("default"));
      assertThat(databases.get(1), is(databaseName));
    } finally {
      client.close();
    }
  }

  private Map<String, String> customConfProperties() {
    return Collections.singletonMap("my.custom.key", "my.custom.value");
  }

  @Test
  public void hiveDefaultName() throws Exception {
    assertExtensionInitialised(defaultDbExtension);
  }

  @Test
  public void hiveCustomName() throws Exception {
    assertExtensionInitialised(customDbExtension);
  }

  @Test
  public void customProperties() {
    HiveConf hiveConf = customPropertiesExtension.conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void createExistingDatabase() {
    assertThrows(AlreadyExistsException.class,
        () -> defaultDbExtension.createDatabase(defaultDbExtension.databaseName()));
  }

  @Test
  public void createDatabaseNullName() {
    assertThrows(NullPointerException.class, () -> defaultDbExtension.createDatabase(null));
  }

  @Test
  public void createDatabaseInvalidName() {
    assertThrows(InvalidObjectException.class, () -> defaultDbExtension.createDatabase(""));
  }

  @Test
  public void thriftPort() {
    int thriftPort = 3333;
    defaultDbExtension.setThriftPort(thriftPort);
    assertThat(defaultDbExtension.getThriftPort(), is(thriftPort));
  }
}
