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

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class BeejuCore {
  private static final Logger LOG = LoggerFactory.getLogger(BeejuCore.class);

  // "user" conflicts with USER db and the metastore_db can't be created.
  public static final String METASTORE_DB_USER = "db_user";
  public static final String METASTORE_DB_PASSWORD = "db_password";

  protected final HiveConf conf = new HiveConf();
  private final String databaseName;
  private final String connectionURL;
  private final String driverClassName;
  private int thriftPort;

  public BeejuCore(String databaseName, Map<String, String> configuration){
    checkNotNull(databaseName, "databaseName is required");
    this.databaseName = databaseName;

    if (configuration != null && !configuration.isEmpty()) {
      for (Map.Entry<String, String> entry : configuration.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    driverClassName = EmbeddedDriver.class.getName();
    conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
    connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";
    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, connectionURL);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, driverClassName);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, METASTORE_DB_USER);
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, METASTORE_DB_PASSWORD);
    conf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, true);
    // Hive 2.x compatibility
    conf.setBoolean("datanucleus.schema.autoCreateAll", true);
    conf.setBoolean("hive.metastore.schema.verification", false);
    // override default port as some of our test environments claim it is in use.
    conf.setInt("hive.server2.webui.port", 0); // ConfVars.HIVE_SERVER2_WEBUI_PORT
    try {
      // overriding default derby log path to go to tmp
      String derbyLog = File.createTempFile("derby", ".log").getCanonicalPath();
      System.setProperty("derby.stream.error.file", derbyLog);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setHiveVar(HiveConf.ConfVars variable, String value){
    conf.setVar(variable, value);
  }

  public void setHiveIntVar(HiveConf.ConfVars variable, int value){
    conf.setIntVar(variable, value);
  }

  /**
   * Create a new database with the specified name.
   *
   * @param databaseName Database name.
   * @throws TException If an error occurs creating the database.
   */
  public void createDatabase(String databaseName, File tempFile, HiveConf conf) throws TException {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(conf));
    String databaseFolder = new File(tempFile, databaseName).toURI().toString();
    try {
      client.createDatabase(new Database(databaseName, null, databaseFolder, null));
    } finally {
      client.close();
    }
  }

  /**
   * @return a copy of the {@link HiveConf} used to create the Hive Metastore database. This {@link HiveConf} should be
   *         used by tests wishing to connect to the database.
   */
  public HiveConf conf() {
    return new HiveConf(conf);
  }

  /**
   * @return the name of the pre-created database.
   */
  public String databaseName() {
    return databaseName;
  }

  /**
   * @return the name of the JDBC driver class used to access the database.
   */
  public String driverClassName() {
    return driverClassName;
  }

  /**
   * @return the JDBC connection URL to the HSQLDB in-memory database.
   */
  public String connectionURL() {
    return connectionURL;
  }

  public void startThrift(ExecutorService thriftServer) throws Exception {
    thriftPort = -1;
    final Lock startLock = new ReentrantLock();
    final Condition startCondition = startLock.newCondition();
    final AtomicBoolean startedServing = new AtomicBoolean();
    try (ServerSocket socket = new ServerSocket(0)) {
      thriftPort = socket.getLocalPort();
    }
    setHiveVar(HiveConf.ConfVars.METASTOREURIS, getThriftConnectionUri());
    final HiveConf hiveConf = new HiveConf(conf(), HiveMetaStoreClient.class);
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

  public void waitForHiveServer2StartUp(HiveServer2 hiveServer2) throws InterruptedException {
    int retries = 0;
    int maxRetries = 5;
    while (hiveServer2.getServiceState() != Service.STATE.STARTED && retries < maxRetries) {
      Thread.sleep(1000);
      retries++;
    }
    if (retries >= maxRetries) {
      throw new RuntimeException("HiveServer2 did not start in a reasonable time");
    }
  }

  /**
   * Creates a new HiveMetaStoreClient that can talk directly to the backed metastore database.
   * <p>
   * The invoker is responsible for closing the client.
   * </p>
   *
   * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
   */
  public HiveMetaStoreClient newClient() {
    try {
      return new HiveMetaStoreClient(conf());
    } catch (MetaException e) {
      throw new RuntimeException("Unable to create HiveMetaStoreClient", e);
    }
  }

  public static class CallableHiveClient implements Callable<HiveMetaStoreClient> {

    private final HiveConf hiveConf;

    public CallableHiveClient(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
    }

    @Override
    public HiveMetaStoreClient call() throws Exception {
      return new HiveMetaStoreClient(hiveConf);
    }
  }

}
