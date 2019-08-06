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
import com.hotels.beeju.core.ThriftHiveMetaStoreCore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class ThriftHiveMetaStoreCoreTest {

  private BeejuCore core = new BeejuCore();
  private ThriftHiveMetaStoreCore thriftHiveMetaStoreCore = new ThriftHiveMetaStoreCore(core);

  @Test
  public void initalisedThriftCore() {
    assertNotNull(thriftHiveMetaStoreCore.getThriftServer());
  }

  @Test
  public void before() throws Exception {
    thriftHiveMetaStoreCore.before();
    assertThat(core.conf().getVar(HiveConf.ConfVars.METASTOREURIS), is(thriftHiveMetaStoreCore.getThriftConnectionUri()));
  }

  @Test

  public void after() {
    thriftHiveMetaStoreCore.after();
    assertTrue(thriftHiveMetaStoreCore.getThriftServer().isShutdown());
  }

}
