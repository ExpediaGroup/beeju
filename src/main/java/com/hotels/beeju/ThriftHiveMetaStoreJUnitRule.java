/**
 * Copyright (C) 2015-2017 Expedia Inc.
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

import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit Rule that creates a Hive Metastore Thrift service backed by a Hive Metastore using an HSQLDB in-memory
 * database.
 * <p/>
 * A fresh database instance will be created for each test method.
 * <p/>
 */
public class ThriftHiveMetaStoreJUnitRule extends HiveMetaStoreJUnitRule {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftHiveMetaStoreJUnitRule.class);

  private final ExecutorService thriftServer;
  private int thriftPort;

  /**
   * Create a Thrift Hive Metastore service with a pre-created database "test_database".
   */
  public ThriftHiveMetaStoreJUnitRule() {
    this("test_database");
  }

  /**
   * Create a Thrift Hive Metastore service with a pre-created database using the provided name.
   *
   * @param databaseName Database name.
   */
  public ThriftHiveMetaStoreJUnitRule(String databaseName) {
    super(databaseName);
    thriftServer = Executors.newSingleThreadExecutor();
  }

  @Override
  protected void before() throws Throwable {
    thriftPort = -1;
    startThrift();
    super.before();
  }

  private void startThrift() throws Exception {
    final Lock startLock = new ReentrantLock();
    final Condition startCondition = startLock.newCondition();
    final AtomicBoolean startedServing = new AtomicBoolean();
    try (ServerSocket s = new ServerSocket(0)) {
      thriftPort = s.getLocalPort();
    }
    conf.setVar(ConfVars.METASTOREURIS, getThriftConnectionUri());
    final HiveConf hiveConf = new HiveConf(conf, HiveMetaStoreClient.class);
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

  @Override
  protected void after() {
    thriftServer.shutdown();
    super.after();
  }

}
