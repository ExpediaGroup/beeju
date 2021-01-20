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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class HiveServer2CoreTest {

  private static final String DATABASE = "my_test_db";
  //private final BeejuCore core = new BeejuCore(DATABASE);
  private HiveServer2Core hiveServer2Core; // = new HiveServer2Core(core);
  
  @AfterEach
  public void cleanup() throws InterruptedException {
    hiveServer2Core.shutdown();
  }
  
  private void setupServer() throws Throwable {
    hiveServer2Core = new HiveServer2Core(new BeejuCore(DATABASE));
    hiveServer2Core.startServerSocket();
    hiveServer2Core.initialise();
    hiveServer2Core.getCore().createDatabase(DATABASE);
  }

  @Test
  public void initiateServer() throws Throwable {
    setupServer();
    assertThat(hiveServer2Core.getJdbcConnectionUrl(),
        is("jdbc:hive2://localhost:" + hiveServer2Core.getPort() + "/" + hiveServer2Core.getCore().databaseName()));
    assertEquals(hiveServer2Core.getCore().conf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT), hiveServer2Core.getPort());
    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STARTED));
  }

  @Test
  public void closeServer() throws Throwable {
    setupServer();
    hiveServer2Core.shutdown();

    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STOPPED));
  }

  @Test
  public void dropTable() throws Throwable {
    setupServer();
    String tableName = "my_table";
    createUnpartitionedTable(DATABASE, tableName, hiveServer2Core);

    try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
        Statement statement = connection.createStatement()) {
      String dropHql = String.format("DROP TABLE %s.%s", DATABASE, tableName);
      statement.execute(dropHql);
    }

    HiveMetaStoreClient client = hiveServer2Core.getCore().newClient();
    try {
      client.getTable(DATABASE, tableName);
      fail(String.format("Table %s.%s was not deleted", DATABASE, tableName));
    } catch (NoSuchObjectException e) {
      // expected
    } finally {
      client.close();
    }
    hiveServer2Core.shutdown();
  }

  @Test
  public void createTable() throws Throwable {
    setupServer();
    String tableName = "my_test_table";

    try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
        Statement statement = connection.createStatement()) {
      String createHql = new StringBuilder()
          .append("CREATE TABLE `" + DATABASE + "." + tableName + "`(`id` int, `name` string) ")
          .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' ")
          .append("STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' ")
          .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
          .toString();
      statement.execute(createHql);
    }

    HiveMetaStoreClient client = hiveServer2Core.getCore().newClient();
    Table table = client.getTable(DATABASE, tableName);
    client.close();
    assertThat(table.getDbName(), is(DATABASE));
    assertThat(table.getTableName(), is(tableName));
    assertThat(table.getSd().getCols(),
        is(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null))));
    assertThat(table.getSd().getInputFormat(), is("org.apache.hadoop.mapred.TextInputFormat"));
    assertThat(table.getSd().getOutputFormat(), is("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"));
    assertThat(table.getSd().getSerdeInfo().getSerializationLib(),
        is("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));
    //server.shutdown();
  }

  @Test
  public void showCreateTable() throws Throwable {
    setupServer();
    String tableName = "my_table";
    Table table = createUnpartitionedTable(DATABASE, tableName, hiveServer2Core);

    StringBuilder showCreateTable = new StringBuilder();
    try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
        Statement statement = connection.createStatement()) {
      String showHql = String.format("SHOW CREATE TABLE %s.%s", DATABASE, tableName);
      ResultSet result = statement.executeQuery(showHql);
      while (result.next()) {
        showCreateTable.append(result.getString(1)).append("\n");
      }
      result.close();
    }
    String expectedShowCreateTable = new StringBuilder()
        .append("CREATE TABLE `my_test_db." + tableName + "`(\n")
        .append("  `id` int, \n")
        .append("  `name` string)\n")
        .append("ROW FORMAT SERDE \n")
        .append("  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n")
        .append("STORED AS INPUTFORMAT \n")
        .append("  'org.apache.hadoop.mapred.TextInputFormat' \n")
        .append("OUTPUTFORMAT \n")
        .append("  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n")
        .append("LOCATION\n")
        .append("  'file:" + hiveServer2Core.getCore().tempDir() + "/" + DATABASE + "/my_table'\n")
        .append("TBLPROPERTIES (\n")
        .append("  'transient_lastDdlTime'='" + table.getParameters().get("transient_lastDdlTime") + "')\n")
        .toString();
    assertThat(showCreateTable.toString(), is(expectedShowCreateTable));
  }

  @Test
  public void dropDatabase() throws Throwable {
    setupServer();
    String databaseName = "Another_DB";

    hiveServer2Core.getCore().createDatabase(databaseName);
    try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
        Statement statement = connection.createStatement()) {
      String dropHql = String.format("DROP DATABASE %s", databaseName);
      statement.execute(dropHql);
    }

    HiveMetaStoreClient client = hiveServer2Core.getCore().newClient();
    try {
      client.getDatabase(databaseName);
      fail(String.format("Database %s was not deleted", databaseName));
    } catch (NoSuchObjectException e) {
      // expected
    } finally {
      client.close();
    }
    //hiveServer2Core.shutdown();
  }

  @Test
  public void addPartition() throws Throwable {
    setupServer();
    String tableName = "my_table";
    createPartitionedTable(DATABASE, tableName, hiveServer2Core);

    try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
        Statement statement = connection.createStatement()) {
      String addPartitionHql = String.format("ALTER TABLE %s.%s ADD PARTITION (partcol=1)", DATABASE, tableName);
      statement.execute(addPartitionHql);
    }

    HiveMetaStoreClient client = hiveServer2Core.getCore().newClient();
    try {
      List<Partition> partitions = client.listPartitions(DATABASE, tableName, (short) -1);
      assertThat(partitions.size(), is(1));
      assertThat(partitions.get(0).getDbName(), is(DATABASE));
      assertThat(partitions.get(0).getTableName(), is(tableName));
      assertThat(partitions.get(0).getValues(), is(Arrays.asList("1")));
      assertThat(partitions.get(0).getSd().getLocation(),
          is(String.format("file:%s/%s/%s/partcol=1", hiveServer2Core.getCore().tempDir(), DATABASE, tableName)));
    } finally {
      client.close();
    }
  }

  @Test
  public void dropPartition() throws Throwable {
    setupServer();
    String tableName = "my_table";
    HiveMetaStoreClient client = hiveServer2Core.getCore().newClient();

    try {
      Table table = createPartitionedTable(DATABASE, tableName, hiveServer2Core);

      Partition partition = new Partition();
      partition.setDbName(DATABASE);
      partition.setTableName(tableName);
      partition.setValues(Arrays.asList("1"));
      partition.setSd(new StorageDescriptor(table.getSd()));
      partition.getSd().setLocation(
          String.format("file:%s/%s/%s/partcol=1", hiveServer2Core.getCore().tempDir(), DATABASE, tableName));
      client.add_partition(partition);

      try (Connection connection = DriverManager.getConnection(hiveServer2Core.getJdbcConnectionUrl());
          Statement statement = connection.createStatement()) {
        String dropPartitionHql = String.format("ALTER TABLE %s.%s DROP PARTITION (partcol=1)", DATABASE, tableName);
        statement.execute(dropPartitionHql);
      }

      List<Partition> partitions = client.listPartitions(DATABASE, tableName, (short) -1);
      assertThat(partitions.size(), is(0));
    } finally {
      client.close();
    }
  }

  private Table createUnpartitionedTable(String databaseName, String tableName, HiveServer2Core server)
      throws Exception {
    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null)));
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    HiveMetaStoreClient client = server.getCore().newClient();
    client.createTable(table);
    client.close();
    return table;
  }

  private Table createPartitionedTable(String databaseName, String tableName, HiveServer2Core server) throws Exception {
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
    HiveMetaStoreClient client = server.getCore().newClient();
    client.createTable(table);
    client.close();
    return table;
  }
}
