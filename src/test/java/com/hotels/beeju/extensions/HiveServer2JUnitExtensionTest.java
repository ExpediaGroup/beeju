/**
 * Copyright (C) 2015-2020 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveServer2JUnitExtensionTest {

  private static final String DATABASE = "my_test_db";

  @RegisterExtension
  HiveServer2JUnitExtension server = new HiveServer2JUnitExtension(DATABASE);

  @Test
  public void defaultDatabaseName() {
    String defaultDbName = new HiveServer2JUnitExtension().databaseName();
    assertThat(defaultDbName, is("test_database"));
  }

  @Test
  public void databaseName() {
    assertThat(server.databaseName(), is(DATABASE));
  }

  @Test
  public void customProperties() {
    Map<String, String> conf = Collections.singletonMap("my.custom.key", "my.custom.value");
    HiveConf hiveConf = new HiveServer2JUnitExtension(DATABASE, conf).conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }
}
