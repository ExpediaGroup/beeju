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

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.beeju.core.HiveMetaStoreCore;

/**
 * A JUnit {@link org.junit.Rule} that creates a Hive Metastore backed by an in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 */
public class HiveMetaStoreJUnitRule extends BeejuJUnitRule {
  
  private static final Logger log = LoggerFactory.getLogger(HiveMetaStoreJUnitRule.class);

  private final HiveMetaStoreCore hiveMetaStoreCore = new HiveMetaStoreCore(core);

  /**
   * Create a Hive Metastore with a pre-created database "test_database".
   */
  public HiveMetaStoreJUnitRule() {
    this("test_database");
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public HiveMetaStoreJUnitRule(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param preConfiguration Hive configuration properties that will be set prior to BeeJU potentially overriding these
   *          with its defaults.
   */
  public HiveMetaStoreJUnitRule(String databaseName, Map<String, String> preConfiguration) {
    super(databaseName, preConfiguration);
  }

  /**
   * Create a Hive Metastore with a pre-created database using the provided name and configuration.
   * 
   * @param databaseName Database name.
   * @param preConfiguration Hive configuration properties that will be set prior to BeeJU potentially overriding these
   *          with its defaults.
   * @param postConfiguration Hive configuration properties that will be set to override BeeJU's defaults.
   */
  public HiveMetaStoreJUnitRule(
      String databaseName,
      Map<String, String> preConfiguration,
      Map<String, String> postConfiguration) {
    super(databaseName, preConfiguration, postConfiguration);
  }

  @Override
  public void starting(Description description) {
    super.starting(description);
    try {
      hiveMetaStoreCore.initialise();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error initialising metastore core", e);
    }
  }

  @Override
  public void finished(Description description) {
    try {
      hiveMetaStoreCore.shutdown();
    } catch (Throwable t) {
      log.warn("Error shutting down metastore core", t);
    }
    super.finished(description);
  }

  /**
   * @return the {@link com.hotels.beeju.core.HiveMetaStoreCore#client()}.
   */
  public HiveMetaStoreClient client() {
    return hiveMetaStoreCore.client();
  }
}
