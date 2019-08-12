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
package com.hotels.beeju.extensions;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;

import com.hotels.beeju.core.HiveMetaStoreCore;

/**
 * A JUnit {@link Extension} that creates a Hive Metastore backed by an HSQLDB in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 * To allow querying of the memory database, add the following to your {@code @BeforeEach} method. This will open a Swing
 * SQL query window for the duration of the test. Remember to add a break point to your test.
 *
 * <pre>
 * &#064;Rule
 * public HiveMetaStoreJUnitRule hive = new HiveMetaStoreJUnitRule();
 *
 * &#064;Override
 * protected void before() throws Throwable {
 *   org.hsqldb.util.DatabaseManagerSwing
 *       .main(new String[] {
 *           &quot;--url&quot;,
 *           hive.connectionURL(),
 *           &quot;--user&quot;,
 *           HiveMetaStoreJUnitRule.HSQLDB_USER,
 *           &quot;--password&quot;,
 *           HiveMetaStoreJUnitRule.HSQLDB_PASSWORD,
 *           &quot;--noexit&quot; });
 *
 * }
 * </pre>
 */
public class HiveMetaStoreJUnitExtension extends BeejuJUnitExtension implements BeforeEachCallback, AfterEachCallback {

  private final HiveMetaStoreCore hiveMetaStoreCore;

  /**
   * Create a Hive Metastore with a pre-created database "test_database".
   */
  HiveMetaStoreJUnitExtension() {
    this("test_database");
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  HiveMetaStoreJUnitExtension(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  HiveMetaStoreJUnitExtension(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
    hiveMetaStoreCore = new HiveMetaStoreCore(core);
  }

  @Override
  public void beforeTest() throws InterruptedException, ExecutionException {
    hiveMetaStoreCore.initialise();
  }

  @Override
  public void afterTest() {
    hiveMetaStoreCore.shutdown();
  }

  /**
   * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
   */
  public HiveMetaStoreClient client() {
    return hiveMetaStoreCore.client();
  }
}
