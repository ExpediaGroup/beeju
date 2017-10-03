/**
 * Copyright (C) 2015-2017 Expedia Inc.
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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class HiveServer2JUnitRuleTest {

  private static final String DATABASE = "my_test_db";

  public @Rule HiveServer2JUnitRule server = new HiveServer2JUnitRule(DATABASE);

  @Before
  public void init() throws Exception {
    Class.forName(server.driverClassName());
  }

  @Test
  public void defaultDatabaseName() {
    String defaultDbName = new HiveServer2JUnitRule().databaseName();
    assertThat(defaultDbName, is("test_database"));
  }

  @Test
  public void customProperties() {
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    HiveConf hiveConf = new HiveServer2JUnitRule(DATABASE, conf).conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void databaseName() {
    assertThat(server.databaseName(), is(DATABASE));
  }

  @Test
  public void showCreateTable() throws Exception {
    String tableName = "my_table";
    Table table = createUnpartitionedTable(DATABASE, tableName);

    StringBuilder showCreateTable = new StringBuilder();
    try (Connection connection = DriverManager.getConnection(server.connectionURL());
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
        .append("  'file:" + server.temporaryFolder.getRoot() + "/" + DATABASE + "/my_table'\n")
        .append("TBLPROPERTIES (\n")
        .append("  'transient_lastDdlTime'='" + table.getParameters().get("transient_lastDdlTime") + "')\n")
        .toString();
    assertThat(showCreateTable.toString(), is(expectedShowCreateTable));
  }

  @Test
  public void createDatabase() throws Exception {
    String databaseName = "Another_DB";

    server.createDatabase(databaseName);
    HiveMetaStoreClient client = server.newClient();
    Database db = client.getDatabase(databaseName);
    client.close();

    assertThat(db, is(notNullValue()));
    assertThat(db.getName(), is(databaseName.toLowerCase()));
    assertThat(db.getLocationUri(), is(String.format("file:%s/%s", server.temporaryFolder.getRoot(), databaseName)));
  }

  @Test
  public void dropDatabase() throws Exception {
    String databaseName = "Another_DB";

    server.createDatabase(databaseName);
    try (Connection connection = DriverManager.getConnection(server.connectionURL());
        Statement statement = connection.createStatement()) {
      String dropHql = String.format("DROP DATABASE %s", databaseName);
      statement.execute(dropHql);
    }

    HiveMetaStoreClient client = server.newClient();
    try {
      client.getDatabase(databaseName);
      fail(String.format("Database %s was not deleted", databaseName));
    } catch (NoSuchObjectException e) {
      // expected
    } finally {
      client.close();
    }
  }

  @Test
  public void createTable() throws Exception {
    String tableName = "my_test_table";

    try (Connection connection = DriverManager.getConnection(server.connectionURL());
        Statement statement = connection.createStatement()) {
      String createHql = new StringBuilder()
          .append("CREATE TABLE `" + DATABASE + "." + tableName + "`(`id` int, `name` string) ")
          .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' ")
          .append("STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' ")
          .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
          .toString();
      statement.execute(createHql);
    }

    HiveMetaStoreClient client = server.newClient();
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
  }

  @Test
  public void dropTable() throws Exception {
    String tableName = "my_table";
    createUnpartitionedTable(DATABASE, tableName);

    try (Connection connection = DriverManager.getConnection(server.connectionURL());
        Statement statement = connection.createStatement()) {
      String dropHql = String.format("DROP TABLE %s.%s", DATABASE, tableName);
      statement.execute(dropHql);
    }

    HiveMetaStoreClient client = server.newClient();
    try {
      client.getTable(DATABASE, tableName);
      fail(String.format("Table %s.%s was not deleted", DATABASE, tableName));
    } catch (NoSuchObjectException e) {
      // expected
    } finally {
      client.close();
    }
  }

  @Test
  public void addPartition() throws Exception {
    String tableName = "my_table";
    createPartitionedTable(DATABASE, tableName);

    try (Connection connection = DriverManager.getConnection(server.connectionURL());
        Statement statement = connection.createStatement()) {
      String addPartitionHql = String.format("ALTER TABLE %s.%s ADD PARTITION (partcol=1)", DATABASE, tableName);
      statement.execute(addPartitionHql);
    }

    HiveMetaStoreClient client = server.newClient();
    try {
      List<Partition> partitions = client.listPartitions(DATABASE, tableName, (short) -1);
      assertThat(partitions.size(), is(1));
      assertThat(partitions.get(0).getDbName(), is(DATABASE));
      assertThat(partitions.get(0).getTableName(), is(tableName));
      assertThat(partitions.get(0).getValues(), is(Arrays.asList("1")));
      assertThat(partitions.get(0).getSd().getLocation(),
          is(String.format("file:%s/%s/%s/partcol=1", server.temporaryFolder.getRoot(), DATABASE, tableName)));
    } finally {
      client.close();
    }
  }

  @Test
  public void dropPartition() throws Exception {
    String tableName = "my_table";
    HiveMetaStoreClient client = server.newClient();

    try {
      Table table = createPartitionedTable(DATABASE, tableName);

      Partition partition = new Partition();
      partition.setDbName(DATABASE);
      partition.setTableName(tableName);
      partition.setValues(Arrays.asList("1"));
      partition.setSd(new StorageDescriptor(table.getSd()));
      partition.getSd().setLocation(
          String.format("file:%s/%s/%s/partcol=1", server.temporaryFolder.getRoot(), DATABASE, tableName));
      client.add_partition(partition);

      try (Connection connection = DriverManager.getConnection(server.connectionURL());
          Statement statement = connection.createStatement()) {
        String addPartitionHql = String.format("ALTER TABLE %s.%s DROP PARTITION (partcol=1)", DATABASE, tableName);
        statement.execute(addPartitionHql);
      }

      List<Partition> partitions = client.listPartitions(DATABASE, tableName, (short) -1);
      assertThat(partitions.size(), is(0));
    } finally {
      client.close();
    }
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
    HiveMetaStoreClient client = server.newClient();
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
    HiveMetaStoreClient client = server.newClient();
    client.createTable(table);
    client.close();
    return table;
  }

}
