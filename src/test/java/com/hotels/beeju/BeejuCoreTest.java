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
package com.hotels.beeju;

import com.hotels.beeju.core.BeejuCore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class BeejuCoreTest {

  private BeejuCore defaultCore = new BeejuCore();
  private BeejuCore dbNameCore = new BeejuCore("test_db");

  @Test
  public void initialisedDefaultConstructor(){
    assertThat(defaultCore.databaseName(), is("test_database"));
  }

  @Test
  public void initialisedDbNameConstructor(){
    assertThat(dbNameCore.databaseName(), is("test_db"));
  }

  @Test
  public void customProperties(){
    Map<String, String> conf = new HashMap<>();
    conf.put("my.custom.key", "my.custom.value");
    HiveConf hiveConf = new BeejuCore("db", conf).conf();
    assertThat(hiveConf.get("my.custom.key"), is("my.custom.value"));
  }

  @Test
  public void setHiveVar(){
    defaultCore.setHiveVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "test");
    assertThat(defaultCore.conf().getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY), is("test"));
  }

  @Test
  public void setHiveIntVar(){
    defaultCore.setHiveIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 00000);
    assertThat(defaultCore.conf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT), is(00000));
  }

}
