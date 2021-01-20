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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.JvmPauseMonitor;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.beeju.hiveserver2.RelaxedSQLStdHiveAuthorizerFactory;

public class HiveServer2Core {
  
  private static final Logger log = LoggerFactory.getLogger(HiveServer2Core.class);

  private final BeejuCore beejuCore;
  private String jdbcConnectionUrl;
  private HiveServer2 hiveServer2;
  private int port;

  public HiveServer2Core(BeejuCore beejuCore) {
    this.beejuCore = beejuCore;
  }
  
  private void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      log.info("Starting HiveServer2");
      HiveConf hiveConf = beejuCore.conf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      long retrySleepIntervalMs = hiveConf
          .getTimeVar(ConfVars.HIVE_SERVER2_SLEEP_INTERVAL_BETWEEN_START_ATTEMPTS,
              TimeUnit.MILLISECONDS);
      hiveServer2 = null;
      try {
        // Cleanup the scratch dir before starting
        ServerUtils.cleanUpScratchDir(hiveConf);
        // Schedule task to cleanup dangling scratch dir periodically,
        // initial wait for a random time between 0-10 min to
        // avoid intial spike when using multiple HS2
        HiveServer2.scheduleClearDanglingScratchDir(hiveConf, new Random().nextInt(600));

        hiveServer2 = new HiveServer2();
        hiveServer2.init(hiveConf);
        hiveServer2.start();

        try {
          JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(hiveConf);
          pauseMonitor.start();
        } catch (Throwable t) {
          log.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
            "warned upon.", t);
        }

        if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
          SparkSessionManagerImpl.getInstance().setup(hiveConf);
        }
        break;
      } catch (Throwable throwable) {
        if (hiveServer2 != null) {
          try {
            hiveServer2.stop();
          } catch (Throwable t) {
            log.info("Exception caught when calling stop of HiveServer2 before retrying start", t);
          } finally {
            hiveServer2 = null;
          }
        }
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          log.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in " + retrySleepIntervalMs + "ms", throwable);
          try {
            Thread.sleep(retrySleepIntervalMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  public void initialise() throws Throwable {
    beejuCore.setHiveVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        RelaxedSQLStdHiveAuthorizerFactory.class.getName());
//    hiveServer2 = new HiveServer2();
//    hiveServer2.init(beejuCore.conf());
//    hiveServer2.start();
    startHiveServer2();
    waitForHiveServer2StartUp();

    jdbcConnectionUrl = "jdbc:hive2://localhost:" + port + "/" + beejuCore.databaseName();
  }

  public void shutdown() {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
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
