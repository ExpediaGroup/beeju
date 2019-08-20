# BeeJU
![Hive Bee JUnit.](logo.png "Project logo of a beeju bee.")
                               
# Start using
You can obtain BeeJU from Maven Central: 

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/beeju/badge.svg?subject=com.hotels:beeju)](https://maven-badges.herokuapp.com/maven-central/com.hotels/beeju) [![Build Status](https://travis-ci.org/HotelsDotCom/beeju.svg?branch=master)](https://travis-ci.org/HotelsDotCom/beeju) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/beeju/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/beeju) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/beeju.svg)

# Overview
BeeJU provides [JUnit4 rules](http://junit.org/junit4/javadoc/4.12/org/junit/Rule.html) that can be used to write test code that tests [Hive](https://hive.apache.org/). A JUnit rule is a means to provide resources in a test and automatically tear them down when the life cycle of a test ends.
This project is currently built with and tested against Hive 2.3.0 (and minor versions back to Hive 1.2.1) but is most likely compatible with older and newer versions of Hive. The available JUnit rules are explained in more detail below.  

Beeju also provides [JUnit5 extensions](https://junit.org/junit5/docs/current/user-guide/#extensions) that can be used in the same manner as the JUnit4 rules with the `junit-jupiter-engine`. Examples of how to switch to JUnit5 can be found below. 
# Usage
The BeeJU JUnit rules and extensions provide a way to run tests that have an underlying requirement to use the Hive Metastore API but don't have the ability to mock the [Hive Metastore Client](https://hive.apache.org/javadocs/r1.2.1/api/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.html). The rules and extensions spin up and tear down an in-memory Metastore which may add a few seconds to the test life cycle so if you require tests to run in the sub-second range this is not for you. 

## Maven Dependencies
Depend on Beeju using:

```xml
    <dependency>
        <groupId>com.hotels</groupId>
        <artifactId>beeju</artifactId>
        <version>....</version>
        <scope>test</scope>
    </dependency>
```

### JUnit4
For JUnit4, ensure you have the JUnit4 dependency in your POM, as Beeju no longer supplies it as a transitive dependency.

### JUnit5 Rule Migration
Support is available to enable you to migrate your tests that currently use Beeju rules without changing them to use extensions. To use JUnit5, ensure you have:
    
```xml
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter-migrationsupport</artifactId>
        <version>${junit.jupiter.version}</version>
        <scope>test</scope>
    </dependency>
```

To any test classes using the Beeju rules, add the class annotation `@EnableRuleMigrationSupport`. No further changes are needed to move your JUnit4 tests to JUnit5.

## ThriftHiveMetaStoreJUnitRule and Extension
This rule creates an in-memory Hive database and a Thrift Hive Metastore service on top of this. This can then be used to perform Hive Thrift API calls in a test. The rule exposes a Thrift URI that can be injected into the class under test and a Hive Metastore Client which can be used for data setup and assertions.

Example `@Rule` usage: Class under test creates a table via the Hive Metastore Thrift API. 

    @Rule
    public ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule("foo_db");
    
    @Test
    public void example() throws Exception {
      ClassUnderTest classUnderTest = new ClassUnderTest(hive.getThriftConnectionUri());
      classUnderTest.createTable("foo_db", "bar_table");	
      
      assertTrue(hive.client().tableExists("foo_db", "bar_table"));
    }

To use the extension instead, replace `@Rule` with:

    @RegisterExtension
    public ThriftHiveMetaStoreJUnitExtension hive = new ThriftHiveMetaStoreJUnitExtension("foo_db");    

## HiveMetaStoreJUnitRule and Extension
This rule creates an in-memory Hive database without a Thrift Hive Metastore service. This can then be used to perform Hive API calls directly (i.e. without going via Hive's Metastore Thrift service) in a test.

Example `@Rule` usage: Class under test creates a partition using an injected Hive Metastore Client. 

    @Rule
    public HiveMetaStoreJUnitRule hive = new HiveMetaStoreJUnitRule("foo_db");
    
    @Test
    public void example() throws Exception {
      HiveMetaStoreClient client = hive.client();
      ClassUnderTest classUnderTest = new ClassUnderTest(client);	
      Table table = new Table();
      table.setDbName("foo_db");
      table.setTableName("bar_table");
      hive.createTable(table);
      
      classUnderTest.createPartition(client, table);
      
      assertEquals(1, client.listPartitions("foo_db", "bar_table", (short) 100));
    }
    
To use the extension instead, replace `@Rule` with:
    
    @RegisterExtension
    public HiveMetaStoreJUnitExtension hive = new HiveMetaStoreJUnitExtension("foo_db");

## HiveServer2JUnitRule and Extension
This rule creates an in-memory Hive database, a Thrift Hive Metastore service on top of this and a HiveServer2 service. This can then be used to perform Hive JDBC calls in a test. The rule exposes a JDBC URI that can be injected into the class under test and a Hive Metastore Client which can be used for data setup and assertions.

Example `@Rule` usage: Class under test drops a table via Hive JDBC.

    @Rule
    public HiveServer2JUnitRule hive = new HiveServer2JUnitRule("foo_db");
    
    @Test
    public void example() {
      Class.forName(hive.driverClassName());
      try (Connection connection = DriverManager.getConnection(hive.connectionURL());
           Statement statement = connection.createStatement()) {
        String createHql = new StringBuilder(256)
            .append("CREATE TABLE `foo_db.bar_table`(`id` int, `name` string) ")
            .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' ")
            .append("STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' ")
            .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
            .toString();
        statement.execute(createHql);
      }
      
      ClassUnderTest classUnderTest = new ClassUnderTest(hive.connectionURL());
      classUnderTest.dropTable("foo_db", "bar_table");  
      
      HiveMetaStoreClient client = hive.newClient();
      try {
        assertFalse(client.tableExists("foo_db", "bar_table"));
      } finally {
        client.close();
      }
    }

To use the extension instead, replace `@Rule` with:
    
    @RegisterExtension
    public HiveServer2Extension hive = new HiveServer2Extension("foo_db");

# Credits

Created by [Dave Maughan](https://github.com/nahguam), [Patrick Duin](https://github.com/patduin) & [Daniel del Castillo](https://github.com/ddcprg) with thanks to: [Adrian Woodhead](https://github.com/massdosage).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2019 Expedia, Inc.
