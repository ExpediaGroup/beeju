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
import java.util.Collections;
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

//TODO: test leaves behind a dery home folder
public class ThriftHiveMetaStoreJUnitRuleTest {

  public @Rule ThriftHiveMetaStoreJUnitRule defaultDbRule = new ThriftHiveMetaStoreJUnitRule();
  public @Rule ThriftHiveMetaStoreJUnitRule customDbRule = new ThriftHiveMetaStoreJUnitRule("my_test_database");
  public @Rule ThriftHiveMetaStoreJUnitRule customPropertiesRule = new ThriftHiveMetaStoreJUnitRule("custom_props_database", customConfProperties());

  private static File defaultTempRoot;
  private static File customTempRoot;
  
  private Map<String, String> customConfProperties() {
    return Collections.singletonMap("my.custom.key", "my.custom.value");
  }

  @Before
  public void before() {
    defaultTempRoot = defaultDbRule.tempDir();
    assertTrue(defaultTempRoot.exists());
    customTempRoot = customDbRule.tempDir();
    assertTrue(customTempRoot.exists());
  }

  @Test
  public void hiveDefaultName() throws Exception {
    assertRuleInitialised(defaultDbRule);
  }

  @Test
  public void hiveCustomName() throws Exception {
    assertRuleInitialised(customDbRule);
  }

  private void assertRuleInitialised(ThriftHiveMetaStoreJUnitRule hive) throws Exception {
    String databaseName = hive.databaseName();

    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.tempDir(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));

    assertThat(hive.getThriftConnectionUri(), is("thrift://localhost:" + hive.getThriftPort()));
    HiveConf conf = new HiveConf(hive.conf());
    conf.setVar(ConfVars.METASTOREURIS, hive.getThriftConnectionUri());
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
      List<String> databases = client.getAllDatabases();
      assertThat(databases.size(), is(2));
      assertThat(databases.get(0), is("default"));
      assertThat(databases.get(1), is(databaseName));
    }
  }

  @Test
  public void customProperties() {
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    HiveConf hiveConf = customPropertiesRule.conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test(expected = AlreadyExistsException.class)
  public void createExistingDatabase() throws TException {
    defaultDbRule.createDatabase(defaultDbRule.databaseName());
  }

  @Test(expected = NullPointerException.class)
  public void createDatabaseNullName() throws TException {
    defaultDbRule.createDatabase(null);
  }

  @Test(expected = InvalidObjectException.class)
  public void createDatabaseInvalidName() throws TException {
    defaultDbRule.createDatabase("");
  }

  @Test
  public void thriftPort() {
    int thriftPort = 3333;
    defaultDbRule.setThriftPort(thriftPort);
    assertThat(defaultDbRule.getThriftPort(), is(thriftPort));
  }

  @AfterClass
  public static void afterClass() {
    assertFalse(defaultTempRoot.exists());
    assertFalse(customTempRoot.exists());
  }

}
