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
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

//TODO: this test leaves one derby home folder behind 
public class HiveMetaStoreJUnitRuleTest {

  private static File tempRoot;
  public @Rule HiveMetaStoreJUnitRule defaultDbRule = new HiveMetaStoreJUnitRule();
  public @Rule HiveMetaStoreJUnitRule customDbRule = new HiveMetaStoreJUnitRule("my_test_database");
  public @Rule HiveMetaStoreJUnitRule customPropertiesRule = new HiveMetaStoreJUnitRule("custom_props_database", customConfProperties());

  private Map<String, String> customConfProperties() {
    return Collections.singletonMap("my.custom.key", "my.custom.value");
  }

  @Before
  public void before() {
    tempRoot = defaultDbRule.tempDir();
    assertTrue(tempRoot.exists());
  }

  @Test
  public void hiveDefaultName() throws Exception {
    assertRuleInitialised(defaultDbRule);
  }

  @Test
  public void hiveCustomName() throws Exception {
    assertRuleInitialised(customDbRule);
  }

  private static void assertRuleInitialised(HiveMetaStoreJUnitRule hive) throws Exception {
    String databaseName = hive.databaseName();
    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.tempDir(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));
  }

  @Test
  public void customProperties() {
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

  @AfterClass
  public static void afterClass() {
    assertFalse("Found folder at " + tempRoot, tempRoot.exists());
  }
}
