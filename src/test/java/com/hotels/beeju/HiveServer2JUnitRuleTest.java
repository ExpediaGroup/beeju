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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class HiveServer2JUnitRuleTest {

  private static final String DATABASE = "my_test_db";

  public @Rule HiveServer2JUnitRule defaultDbRule = new HiveServer2JUnitRule();
  public @Rule HiveServer2JUnitRule customDbRule = new HiveServer2JUnitRule(DATABASE);
  public @Rule HiveServer2JUnitRule customPropertiesRule = new HiveServer2JUnitRule("custom_props_database", customConfProperties());
  
  private Map<String, String> customConfProperties() {
    return Collections.singletonMap("my.custom.key", "my.custom.value");
  }

  @Before
  public void init() throws Exception {
    Class.forName(customDbRule.driverClassName());
  }

  @Test
  public void defaultDatabaseName() {
    String defaultDbName = defaultDbRule.databaseName();
    assertThat(defaultDbName, is("test_database"));
  }

  @Test
  public void customProperties() {
    HiveConf hiveConf = customPropertiesRule.conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void databaseName() {
    assertThat(customDbRule.databaseName(), is(DATABASE));
  }
}
