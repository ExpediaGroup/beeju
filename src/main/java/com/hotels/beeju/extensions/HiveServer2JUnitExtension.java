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
package com.hotels.beeju.extensions;

import java.util.Map;

import org.apache.hive.jdbc.HiveDriver;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.hotels.beeju.core.HiveServer2Core;

public class HiveServer2JUnitExtension extends BeejuJUnitExtension {

  private final HiveServer2Core hiveServer2Core;

  /**
   * Create a HiveServer2 service with a pre-created database "test_database".
   */
  public HiveServer2JUnitExtension() {
    this("test_database");
  }

  /**
   * Create a HiveServer2 service with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public HiveServer2JUnitExtension(String databaseName) {
    this(databaseName, null);
  }

  /**
   * Create a HiveServer2 service with a pre-created database using the provided name and configuration.
   *
   * @param databaseName Database name.
   * @param configuration Hive configuration properties.
   */
  public HiveServer2JUnitExtension(String databaseName, Map<String, String> configuration) {
    super(databaseName, configuration);
    hiveServer2Core = new HiveServer2Core(core);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    hiveServer2Core.startServerSocket();
    super.beforeEach(context);
    try {
      hiveServer2Core.initialise();
    } catch (Throwable e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    hiveServer2Core.shutdown();
    super.afterEach(context);
  }

  /**
   * @return the name of the Hive JDBC driver class used to access the database.
   */
  @Override
  public String driverClassName() {
    return HiveDriver.class.getName();
  }

  /**
   * @return {@link com.hotels.beeju.core.HiveServer2Core#getJdbcConnectionUrl()}.
   */
  @Override
  public String connectionURL() {
    return hiveServer2Core.getJdbcConnectionUrl();
  }
}
