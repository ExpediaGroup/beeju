#BeeJU
![Hive Bee JUnit.](logo.png "Project logo of a bijou bee.")
                               
#Start using
You can obtain BeeJU from Maven Central : 

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/beeju/badge.svg?subject=com.hotels:beeju)](https://maven-badges.herokuapp.com/maven-central/com.hotels/beeju) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/beeju.svg)

#Overview
BeeJU provides [JUnit rules](http://junit.org/junit4/javadoc/4.12/org/junit/Rule.html) that can be used to write test code that tests [Hive](https://hive.apache.org/). A JUnit rule is a means to provide resources in a test and automatically tear them down when the life cycle of a test ends.
This project is currently built with and tested against Hive 1.2.1 but is most likely compatible with older and newer versions of Hive. The available JUnit rules are explained in more detail below.  

#Usage
The BeeJU JUnit rules provide a way to run tests that have an underlying requirement to use the Hive Metastore API but don't have the ability to mock the [Hive Metastore Client](https://hive.apache.org/javadocs/r1.2.1/api/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.html). The rules spin up and tear down an in-memory Metastore which may add few seconds to the test life cycle so if you require tests to run in the sub-second range this is not for you.
  
##ThriftHiveMetaStoreJUnitRule
This rule creates an in-memory Hive database and a Thrift Hive Metastore service on top of this. This can then be used to perform Hive Thrift API calls in a test. The rule exposes a Thrift URI that can be injected into the class under test and a Hive Metastore Client which can be used for data setup and assertions.

Example usage: Class under test creates a table via the Hive Metastore Thrift API. 

	@Rule
	public ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule("foo_db");
	
	@Test
	public void example() {
	  ClassUnderTest classUnderTest = new ClassUnderTest(hive.getThriftConnectionUri());
	  classUnderTest.createTable("foo_db", "bar_table");	

      assertTrue(hive.client().tableExists("foo_db", "bar_table"));
	}

##HiveMetaStoreJUnitRule
This rule creates an in-memory Hive database without a Thrift Hive Metastore service. This can then be used to perform Hive API calls directly (i.e. without going via Hive's Metastore Thrift service) in a test.

Example usage: Class under test creates a partition using an injected Hive Metastore Client. 

	@Rule
	public HiveMetaStoreJUnitRule hive = new HiveMetaStoreJUnitRule("foo_db");
	
	@Test
	public void example() {
	  HiveMetaStoreClient client = hive.client();
	  ClassUnderTest classUnderTest = new ClassUnderTest(client);	
	  Table table = new Table();
      table.setDbName("foo_db");
      table.setTableName("bar_table");
      hive.createTable(table);

      classUnderTest.createPartition(client, table);

      assertEquals(1, client.listPartitions("foo_db", "bar_table", 100));
	}

#Credits

Created by [Dave Maughan](https://github.com/nahguam), [Patrick Duin](https://github.com/patduin) & [Daniel del Castillo](https://github.com/ddcprg) with thanks to: [Adrian Woodhead](https://github.com/massdosage).

#Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016 Expedia Inc.
