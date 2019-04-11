/**
 * Copyright (C) 2015-2017 Expedia, Inc.
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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessController;

/**
 * This class unsets the owner privileges to avoid issues with the the persistence manager.
 * <p>
 * By default SELECT, INSERT, UPDATE and DELETE rights are granted to the owner and in our set up this caused the
 * persistence manager to hang. We don't need these rights because this service is just for testing so we remove them
 * here.
 * </p>
 */
public class RelaxedSQLStdHiveAccessController extends SQLStdHiveAccessController {

  public RelaxedSQLStdHiveAccessController(
      HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf,
      HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
    super(metastoreClientFactory, conf, authenticator, ctx);
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    super.applyAuthorizationConfigPolicy(hiveConf);
    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS, "");
  }

}
