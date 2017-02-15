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
package com.hotels.beeju.hiveserver2;

import java.lang.reflect.Field;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessControllerWrapper;

/**
 * This class overrides the {@code hiveAccessController} attribute in the parent class with our own implementation.
 */
public class RelaxedSQLStdHiveAccessControllerWrapper extends SQLStdHiveAccessControllerWrapper {

  public RelaxedSQLStdHiveAccessControllerWrapper(
      HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf,
      HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
    super(metastoreClientFactory, conf, authenticator, ctx);
    overrideHiveAccessController(
        new RelaxedSQLStdHiveAccessController(metastoreClientFactory, conf, authenticator, ctx));
  }

  private void overrideHiveAccessController(RelaxedSQLStdHiveAccessController hackedSQLStdHiveAccessController) {
    try {
      Field hiveAccessControllerField = SQLStdHiveAccessControllerWrapper.class
          .getDeclaredField("hiveAccessController");
      hiveAccessControllerField.setAccessible(true);
      hiveAccessControllerField.set(this, hackedSQLStdHiveAccessController);
      hiveAccessControllerField.setAccessible(false);
    } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
      throw new RuntimeException("Unable to override field 'hiveAccessController' in class "
          + SQLStdHiveAccessControllerWrapper.class.getName(), e);
    }
  }

}
