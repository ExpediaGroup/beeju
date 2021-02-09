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
package com.hotels.beeju.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class BeejuCoreTest {

  private String preKey = "my.custom.pre.key";
  private String preValue = "my.custom.pre.value";
  private String postKey = "my.custom.post.key";
  private String postValue = "my.custom.post.value";
  private String coreOverrideValue = "user-that-core-will-override";
  private String confOverrideValue = "password-that-will-override-core";

  private final BeejuCore defaultCore = new BeejuCore();
  private final BeejuCore dbNameCore = new BeejuCore("test_db");
  private final BeejuCore dbNameAndMapConfCore = new BeejuCore("test_db_2", createPreConfigurationMap(),
      createPostConfigurationMap());
  private final BeejuCore dbNameAndHiveConfCore = new BeejuCore("test_db_2", createPreHiveConf(), createPostHiveConf());

  private Map<String, String> createPreConfigurationMap() {
    Map<String, String> conf = new HashMap<>();
    conf.put(preKey, preValue);
    conf.put(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), coreOverrideValue);
    return conf;
  }

  private Map<String, String> createPostConfigurationMap() {
    Map<String, String> conf = new HashMap<>();
    conf.put(postKey, postValue);
    conf.put(HiveConf.ConfVars.METASTOREPWD.toString(), confOverrideValue);
    return conf;
  }

  private HiveConf createPreHiveConf() {
    HiveConf conf = new HiveConf();
    conf.clear();
    conf.set(preKey, preValue);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, coreOverrideValue);
    return conf;
  }

  private HiveConf createPostHiveConf() {
    HiveConf conf = new HiveConf();
    conf.clear();
    conf.set(postKey, postValue);
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, confOverrideValue);
    return conf;
  }

  @AfterEach
  public void cleanUp() {
    defaultCore.cleanUp();
    dbNameCore.cleanUp();
    dbNameAndMapConfCore.cleanUp();
    dbNameAndHiveConfCore.cleanUp();
  }

  @Test
  public void initialisedDefaultConstructor() {
    assertThat(defaultCore.databaseName(), is("test_database"));
  }

  @Test
  public void initialisedDbNameConstructor() {
    assertThat(dbNameCore.databaseName(), is("test_db"));
  }

  @Test
  public void initialisedDbNameAndMapConfConstructor() {
    assertThat(dbNameAndMapConfCore.databaseName(), is("test_db_2"));
    assertThat(dbNameAndMapConfCore.conf().get(preKey), is(preValue));
    assertThat(dbNameAndMapConfCore.conf().get(postKey), is(postValue));
    // below still set to BeeJU default as pre-config not overridden
    assertThat(dbNameAndMapConfCore.conf().getVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME), is("db_user"));
    // below overridden by post-config
    assertThat(dbNameAndMapConfCore.conf().getVar(HiveConf.ConfVars.METASTOREPWD), is(confOverrideValue));
  }

  @Test
  public void initialisedDbNameAndHiveConfConstructor() {
    assertThat(dbNameAndHiveConfCore.databaseName(), is("test_db_2"));
    assertThat(dbNameAndHiveConfCore.conf().get(preKey), is(preValue));
    assertThat(dbNameAndHiveConfCore.conf().get(postKey), is(postValue));
    // below still set to BeeJU default as pre-config not overridden
    assertThat(dbNameAndHiveConfCore.conf().getVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME), is("db_user"));
    // below overridden by post-config
    assertThat(dbNameAndHiveConfCore.conf().getVar(HiveConf.ConfVars.METASTOREPWD), is(confOverrideValue));
  }

  @Test
  public void createDirectory() {
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE), is(defaultCore.warehouseDir().toString()));
  }

  @Test
  public void deleteDirectory() throws IOException {
    BeejuCore testCore = new BeejuCore();
    testCore.cleanUp();
    assertFalse(Files.exists(testCore.warehouseDir()));
    assertFalse(Files.exists(testCore.tempDir()));
  }

  @Test
  public void setHiveVar() {
    defaultCore.setHiveVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "test");
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY), is("test"));
  }

  @Test
  public void setHiveConf() {
    defaultCore.setHiveConf("my.custom.key", "test");
    assertThat(defaultCore.conf().get("my.custom.key"), is("test"));
  }

  @Test
  public void setHiveIntVar() {
    defaultCore.setHiveIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 00000);
    assertThat(defaultCore.conf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT), is(00000));
  }

  @Test
  public void checkConfig() {
    assertThat(defaultCore.driverClassName(), is(EmbeddedDriver.class.getName()));
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY), is(defaultCore.connectionURL()));
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER),
        is(defaultCore.driverClassName()));
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME), is("db_user"));
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTOREPWD), is("db_password"));
    assertThat(defaultCore.conf().getBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF), is(true));
    assertThat(defaultCore.conf().get("datanucleus.schema.autoCreateAll"), is("true"));
    assertThat(defaultCore.conf().get("hive.metastore.schema.verification"), is("false"));
    assertThat(defaultCore.conf().get("hcatalog.hive.client.cache.disabled"), is("true"));
  }

  @Test
  public void createDatabase() throws Exception {
    String databaseName = "Another_DB";

    defaultCore.createDatabase(databaseName);
    HiveMetaStoreClient client = defaultCore.newClient();
    Database db = client.getDatabase(databaseName);
    client.close();

    assertThat(db, is(notNullValue()));
    assertThat(db.getName(), is(databaseName.toLowerCase()));
    assertThat(db.getLocationUri(), is(String.format("file:%s/%s", defaultCore.warehouseDir(), databaseName)));
  }
  
}
