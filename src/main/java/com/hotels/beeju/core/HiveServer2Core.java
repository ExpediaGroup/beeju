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
package com.hotels.beeju.core;



import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

import com.hotels.beeju.hiveserver2.RelaxedSQLStdHiveAuthorizerFactory;

public class HiveServer2Core {

  private final BeejuCore beejuCore;
  private String jdbcConnectionUrl;
  private HiveServer2 hiveServer2;
  private int port;

  public HiveServer2Core(BeejuCore beejuCore) {
    this.beejuCore = beejuCore;
  }

  public void initialise() throws InterruptedException {
    System.setProperty(CONNECT_URL_KEY.getVarname(), beejuCore.conf.get(CONNECT_URL_KEY.getVarname()));
    beejuCore.setHiveVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        RelaxedSQLStdHiveAuthorizerFactory.class.getName());
    hiveServer2 = new HiveServer2();
    hiveServer2.init(beejuCore.conf());
    hiveServer2.start();
    waitForHiveServer2StartUp();

    jdbcConnectionUrl = "jdbc:hive2://localhost:" + port + "/" + beejuCore.databaseName();
  }

  public void shutdown() {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }

    System.clearProperty(CONNECT_URL_KEY.getVarname());
  }

  private void waitForHiveServer2StartUp() throws InterruptedException {
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

  public void startServerSocket() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    beejuCore.setHiveIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
  }

  public String getJdbcConnectionUrl() {
    return jdbcConnectionUrl;
  }

  public int getPort() {
    return port;
  }

  public HiveServer2 getHiveServer2() {
    return hiveServer2;
  }

  public BeejuCore getCore() {
    return beejuCore;
  }
}
