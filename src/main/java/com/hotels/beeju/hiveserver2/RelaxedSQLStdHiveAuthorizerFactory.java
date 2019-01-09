/**
 * Copyright (C) 2015-2017 The Apache Software Foundation and Expedia Inc.
 * This code is based on Hive's HiveMetaStore:
 *
 * https://github.com/apache/hive/blob/release-2.1.0-rc3/ql/src/java/org/apache/hadoop/hive/ql/
 * security/authorization/plugin/sqlstd/SQLStdHiveAuthorizerFactory.java
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.beeju.hiveserver2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizationValidator;

/**
 * Based on {@code org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessControllerWrapper} in
 * {@code hive-exec-1.2.1}
 * <p>
 * This class creates a {@code HiveAuthorizer} that ignores the default owner privileges on the tables.
 * </p>
 */
public class RelaxedSQLStdHiveAuthorizerFactory implements HiveAuthorizerFactory {
  @Override
  public HiveAuthorizer createHiveAuthorizer(
      HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf,
      HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx)
    throws HiveAuthzPluginException {
    RelaxedSQLStdHiveAccessControllerWrapper privilegeManager = new RelaxedSQLStdHiveAccessControllerWrapper(
        metastoreClientFactory, conf, authenticator, ctx);
    return new HiveAuthorizerImpl(privilegeManager,
        new SQLStdHiveAuthorizationValidator(metastoreClientFactory, conf, authenticator, privilegeManager, ctx));
  }
}
