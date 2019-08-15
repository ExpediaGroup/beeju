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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.Service;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HiveServer2CoreTest {

  private static final String DATABASE = "my_test_db";
  private final BeejuCore core = new BeejuCore(DATABASE);
  private final HiveServer2Core hiveServer2Core = new HiveServer2Core(core);

  public TemporaryFolder temporaryFolder;

  @Before
  public void setup(){
    temporaryFolder = new TemporaryFolder();
  }

  @Test
  public void initiateServer() throws InterruptedException {
    hiveServer2Core.initialise();
    assertThat(hiveServer2Core.getJdbcConnectionUrl(),
        is("jdbc:hive2://localhost:" + hiveServer2Core.getPort() + "/" + core.databaseName()));
    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STARTED));
  }

  @Test
  public void closeServer() throws InterruptedException, IOException {
    hiveServer2Core.startServerSocket();
    hiveServer2Core.initialise();
    hiveServer2Core.shutdown();

    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STOPPED));
  }

  @Test
  public void startServerSocket() throws IOException {
    hiveServer2Core.startServerSocket();
    assertEquals(core.conf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT), hiveServer2Core.getPort());
  }

  private Table createUnpartitionedTable(String databaseName, String tableName) throws Exception {
    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null)));
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    HiveMetaStoreClient client = core.newClient();
    client.createTable(table);
    client.close();
    return table;
  }

  private Table createPartitionedTable(String databaseName, String tableName) throws Exception {
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(tableName);
    table.setPartitionKeys(Arrays.asList(new FieldSchema("partcol", "int", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null)));
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    HiveMetaStoreClient client = core.newClient();
    client.createTable(table);
    client.close();
    return table;
  }
}
