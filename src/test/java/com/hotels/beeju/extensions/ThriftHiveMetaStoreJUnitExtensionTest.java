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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRuleTest;

public class ThriftHiveMetaStoreJUnitExtensionTest{

  @RegisterExtension
  ThriftHiveMetaStoreJUnitExtension hiveDefaultName = new ThriftHiveMetaStoreJUnitExtension();

  @RegisterExtension
  ThriftHiveMetaStoreJUnitExtension hiveCustomName = new ThriftHiveMetaStoreJUnitExtension("my_test_database");

  private void assertRuleInitialised(ThriftHiveMetaStoreJUnitExtension hive) throws Exception {
    String databaseName = hive.databaseName();

    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.getTempDirectory(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));

    assertThat(hive.getThriftConnectionUri(), is("thrift://localhost:" + hive.getThriftPort()));
    HiveConf conf = new HiveConf(ThriftHiveMetaStoreJUnitRuleTest.class);
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, hive.getThriftConnectionUri());
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    List<String> databases = client.getAllDatabases();
    assertThat(databases.size(), is(2));
    assertThat(databases.get(0), is("default"));
    assertThat(databases.get(1), is(databaseName));
  }

  @Test
  public void hiveDefaultName() throws Exception {
    assertRuleInitialised(hiveDefaultName);
  }

  @Test
  public void hiveCustomName() throws Exception {
    assertRuleInitialised(hiveCustomName);
  }

  @Test
  public void customProperties() {
    Map<String, String> conf = Collections.singletonMap("my.custom.key", "my.custom.value");
    HiveConf hiveConf = new ThriftHiveMetaStoreJUnitRule("db", conf).conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void createExistingDatabase() {
    assertThrows(AlreadyExistsException.class, ()
        -> hiveDefaultName.createDatabase(hiveDefaultName.databaseName()));
  }

  @Test
  public void createDatabaseNullName() {
    assertThrows(NullPointerException.class, () -> hiveDefaultName.createDatabase(null));
  }

  @Test
  public void createDatabaseInvalidName() {
    assertThrows(InvalidObjectException.class, () -> hiveDefaultName.createDatabase(""));
  }
}
