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
package com.hotels.beeju.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.junit.Test;

public class HiveServer2CoreTest {

  private final BeejuCore core = new BeejuCore();
  private final HiveServer2Core hiveServer2Core = new HiveServer2Core(core);

  @Test
  public void initiateServer() throws InterruptedException {
    hiveServer2Core.initialise();
    assertThat(hiveServer2Core.getJdbcConnectionUrl(),
        is("jdbc:hive2://localhost:" + hiveServer2Core.getPort() + "/" + core.databaseName()));
    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STARTED));
  }

  @Test
  public void closeServer() throws InterruptedException {
    hiveServer2Core.initialise();
    hiveServer2Core.shutdown();

    assertThat(hiveServer2Core.getHiveServer2().getServiceState(), is(Service.STATE.STOPPED));
  }

  @Test
  public void startServerSocket() throws IOException {
    hiveServer2Core.startServerSocket();
    assertEquals(core.conf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT), hiveServer2Core.getPort());
  }
}
