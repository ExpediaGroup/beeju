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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveMetaStoreJUnitExtensionTest {

  @RegisterExtension
  HiveMetaStoreJUnitExtension hiveDefaultName = new HiveMetaStoreJUnitExtension();

  @RegisterExtension
  HiveMetaStoreJUnitExtension hiveCustomDbName = new HiveMetaStoreJUnitExtension("my_test_database");

  @RegisterExtension
  HiveMetaStoreJUnitExtension hiveCustomDbNameAndConf = new HiveMetaStoreJUnitExtension("my_test_database",
      customConfProperties());

  private static void assertRuleInitialised(HiveMetaStoreJUnitExtension hive) throws Exception {
    String databaseName = hive.databaseName();
    Database database = hive.client().getDatabase(databaseName);

    assertThat(database.getName(), is(databaseName));
    File databaseFolder = new File(hive.getTempDirectory(), databaseName);
    assertThat(new File(database.getLocationUri()) + "/", is(databaseFolder.toURI().toString()));
  }

  private Map<String, String> customConfProperties() {
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    return conf;
  }

  @Test
  public void defaultDbNameInitialised() throws Exception {
    assertRuleInitialised(hiveDefaultName);
  }

  @Test
  public void customDbNameInitialised() throws Exception {
    assertRuleInitialised(hiveCustomDbName);
  }

  @Test
  public void customDbNameAndConfInitialised() throws Exception {
    assertRuleInitialised(hiveCustomDbNameAndConf);
  }

  @Test
  public void createExistingDatabase() {
    Assertions.assertThrows(AlreadyExistsException.class, () -> {
      hiveDefaultName.createDatabase(hiveDefaultName.databaseName());
    });
  }

  @Test
  public void createDatabaseNullName() {
    Assertions.assertThrows(NullPointerException.class, () -> {
      hiveDefaultName.createDatabase(null);
    });
  }
}
