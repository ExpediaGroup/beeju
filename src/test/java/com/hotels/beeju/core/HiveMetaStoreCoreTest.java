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

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class HiveMetaStoreCoreTest {

  private final BeejuCore core = new BeejuCore();
  private final HiveMetaStoreCore hiveMetaStoreCore = new HiveMetaStoreCore(core);

  @Test
  public void clientStarted() throws ExecutionException, InterruptedException {
    hiveMetaStoreCore.initialise();
    assertNotNull(hiveMetaStoreCore.client());
  }
}