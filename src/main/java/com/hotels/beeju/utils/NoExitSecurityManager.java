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
package com.hotels.beeju.utils;

import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;

public class NoExitSecurityManager extends SecurityManager {

  private boolean isExitAllowedFlag;

  public NoExitSecurityManager() {
    super();
    isExitAllowedFlag = false;
  }

  public boolean isExitAllowed() {
    return isExitAllowedFlag;
  }

  @Override
  public void checkExit(int status) {
    if (!isExitAllowed()) {
      throw new SecurityException("SecurityException: System.exit call intercepted and ignored.");
    }
    super.checkExit(status);
  }

  public void setExitAllowed(boolean f) {
    isExitAllowedFlag = f;
  }

  public void setPolicy() {
    Policy.getPolicy();

    Policy allPermissionPolicy = new Policy() {
      @Override
      public boolean implies(ProtectionDomain domain, Permission permission) {
        return true;
      }
    };

    Policy.setPolicy(allPermissionPolicy);
  }
}
