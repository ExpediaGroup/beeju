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
package com.hotels.beeju.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.Test;

public class BeejuCoreTest {

  private final BeejuCore defaultCore = new BeejuCore();
  private final BeejuCore dbNameCore = new BeejuCore("test_db");
  private final BeejuCore dbNameAndConfCore = new BeejuCore("test_db_2", createConf());

  private Map<String, String> createConf() {
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    return conf;
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
  public void intialisedDbNameAndConfConstructor() {
    assertThat(dbNameAndConfCore.databaseName(), is("test_db_2"));
    assertThat(dbNameAndConfCore.conf().get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void setHiveVar() {
    defaultCore.setHiveVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "test");
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY), is("test"));
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
    assertThat(defaultCore.conf().get("hive.server2.webui.port"), is("0"));
    assertThat(defaultCore.conf().get("hcatalog.hive.client.cache.disabled"), is("true"));
  }

//  @Test
//  public void createDatabase() throws Exception {
//    String databaseName = "Another_DB";
//
//    defaultCore.createDatabase(databaseName);
//    HiveMetaStoreClient client = defaultCore.newClient();
//    Database db = client.getDatabase(databaseName);
//    client.close();
//
//    assertThat(db, is(notNullValue()));
//    assertThat(db.getName(), is(databaseName.toLowerCase()));
//    assertThat(db.getLocationUri(), is(String.format("file:%s/%s", temporaryFolder, databaseName)));
//  }
}
