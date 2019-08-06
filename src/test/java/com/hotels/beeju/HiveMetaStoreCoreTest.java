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
import com.hotels.beeju.core.HiveMetaStoreCore;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class HiveMetaStoreCoreTest {

  private BeejuCore core = new BeejuCore();
  private HiveMetaStoreCore hiveMetaStoreCore = new HiveMetaStoreCore(core);

  @Test
  public void clientStarted() throws ExecutionException, InterruptedException {
    hiveMetaStoreCore.before();
    assertNotNull(hiveMetaStoreCore.client());
  }

  @Test(expected = NoSuchObjectException.class)
  public void clientClosed() throws Exception {
    hiveMetaStoreCore.before();
    hiveMetaStoreCore.after();
    hiveMetaStoreCore.client().getDatabase(core.databaseName());

  }
}
