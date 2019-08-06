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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThriftHiveMetaStoreCore {

  private int thriftPort;
  private final ExecutorService thriftServer;
  private BeejuCore beejuCore;

  private static final Logger LOG = LoggerFactory.getLogger(ThriftHiveMetaStoreCore.class);

  public ThriftHiveMetaStoreCore(BeejuCore beejuCore){
    this.beejuCore = beejuCore;
    this.thriftServer = Executors.newSingleThreadExecutor();
  }

  public void before() throws Exception {
    thriftPort = -1;
    final Lock startLock = new ReentrantLock();
    final Condition startCondition = startLock.newCondition();
    final AtomicBoolean startedServing = new AtomicBoolean();
    try (ServerSocket socket = new ServerSocket(0)) {
      thriftPort = socket.getLocalPort();
    }
    beejuCore.setHiveVar(HiveConf.ConfVars.METASTOREURIS, getThriftConnectionUri());
    final HiveConf hiveConf = new HiveConf(beejuCore.conf(), HiveMetaStoreClient.class);
    thriftServer.execute(new Runnable() {
      @Override
      public void run() {
        try {
          HadoopThriftAuthBridge bridge = new HadoopThriftAuthBridge23();
          HiveMetaStore.startMetaStore(thriftPort, bridge, hiveConf, startLock, startCondition, startedServing);
        } catch (Throwable e) {
          LOG.error("Unable to start a Thrift server for Hive Metastore", e);
        }
      }
    });
    int i = 0;
    while (i++ < 3) {
      startLock.lock();
      try {
        if (startCondition.await(1, TimeUnit.MINUTES)) {
          break;
        }
      } finally {
        startLock.unlock();
      }
      if (i == 3) {
        throw new RuntimeException("Maximum number of tries reached whilst waiting for Thrift server to be ready");
      }
    }
  }

  public void after(){
    thriftServer.shutdown();
  }

  /**
   * @return The Thrift connection {@link URI} string for the Metastore service.
   */
  public String getThriftConnectionUri() {
    return "thrift://localhost:" + thriftPort;
  }

  /**
   * @return The port used for the Thrift Metastore service.
   */
  public int getThriftPort() {
    return thriftPort;
  }

  public ExecutorService getThriftServer(){
    return thriftServer;
  }
}
