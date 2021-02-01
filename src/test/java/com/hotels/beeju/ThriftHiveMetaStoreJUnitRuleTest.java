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
package com.hotels.beeju;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ThriftHiveMetaStoreJUnitRuleTest {

  public @Rule ThriftHiveMetaStoreJUnitRule hiveDefaultName = new ThriftHiveMetaStoreJUnitRule();

  public @Rule ThriftHiveMetaStoreJUnitRule hiveCustomName = new ThriftHiveMetaStoreJUnitRule("my_test_database");

  private static File defaultTempRoot;
  private static File customTempRoot;

  @Before
  public void before() {
    defaultTempRoot = hiveDefaultName.tempDir();
    assertTrue(defaultTempRoot.exists());
    customTempRoot = hiveCustomName.tempDir();
    assertTrue(customTempRoot.exists());
  }

  @Test
  public void hiveDefaultName() throws Exception {
    assertRuleInitialised(hiveDefaultName);
  }

  @Test
  public void hiveCustomName() throws Exception {
    assertRuleInitialised(hiveCustomName);
  }

  private void assertRuleInitialised(ThriftHiveMetaStoreJUnitRule hive) throws Exception {
    String databaseName = hive.databaseName();

    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.tempDir(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));

    assertThat(hive.getThriftConnectionUri(), is("thrift://localhost:" + hive.getThriftPort()));
    HiveConf conf = new HiveConf(ThriftHiveMetaStoreJUnitRuleTest.class);
    conf.setVar(ConfVars.METASTOREURIS, hive.getThriftConnectionUri());
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    List<String> databases = client.getAllDatabases();
    assertThat(databases.size(), is(2));
    assertThat(databases.get(0), is("default"));
    assertThat(databases.get(1), is(databaseName));
  }

  @Test
  public void customProperties() {
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    HiveConf hiveConf = new ThriftHiveMetaStoreJUnitRule("db", conf).conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test(expected = AlreadyExistsException.class)
  public void createExistingDatabase() throws TException {
    hiveDefaultName.createDatabase(hiveDefaultName.databaseName());
  }

  @Test(expected = NullPointerException.class)
  public void createDatabaseNullName() throws TException {
    hiveDefaultName.createDatabase(null);
  }

  @Test(expected = InvalidObjectException.class)
  public void createDatabaseInvalidName() throws TException {
    hiveDefaultName.createDatabase("");
  }

  @Test
  public void thriftPort() {
    int thriftPort = 3333;
    hiveDefaultName.setThriftPort(thriftPort);
    assertThat(hiveDefaultName.getThriftPort(), is(thriftPort));
  }

  @AfterClass
  public static void afterClass() {
    assertFalse(defaultTempRoot.exists());
    assertFalse(customTempRoot.exists());
  }

}
