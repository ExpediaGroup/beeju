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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

//TODO: test leaves behind a derby home folder
public class HiveMetaStoreJUnitExtensionTest {

  @RegisterExtension
  HiveMetaStoreJUnitExtension defaultDbExtension = new HiveMetaStoreJUnitExtension();

  @RegisterExtension
  HiveMetaStoreJUnitExtension customDbExtension = new HiveMetaStoreJUnitExtension("my_test_database");

  @RegisterExtension
  HiveMetaStoreJUnitExtension customPropertiesExtension = new HiveMetaStoreJUnitExtension("custom_props_database",
      customConfProperties());

  private void assertExtensionInitialised(HiveMetaStoreJUnitExtension hive) throws Exception {
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
    assertExtensionInitialised(defaultDbExtension);
  }

  @Test
  public void customDbNameInitialised() throws Exception {
    assertExtensionInitialised(customDbExtension);
  }

  @Test
  public void customDbNameAndConfInitialised() throws Exception {
    assertExtensionInitialised(customPropertiesExtension);
  }

  @Test
  public void createExistingDatabase() {
    assertThrows(AlreadyExistsException.class,
        () -> defaultDbExtension.createDatabase(defaultDbExtension.databaseName()));
  }

  @Test
  public void createDatabaseNullName() {
    assertThrows(NullPointerException.class, () -> defaultDbExtension.createDatabase(null));
  }
}
