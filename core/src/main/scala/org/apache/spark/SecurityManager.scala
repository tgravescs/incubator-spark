/*
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

package org.apache.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil

/** 
 * Spark class responsible for security.  
 */
private object SecurityManager extends Logging {

  def isUIAuthenticationEnabled(): Boolean = {
    System.getProperty("spark.authenticate.ui", "false").toBoolean
  }

  // allow anyone in the acl list and the application owner 
  def checkUIViewPermissions(user: String): Boolean = {
    val viewAcls = System.getProperty("spark.ui.view.acls", "").split(',').map(_.trim()).toSet
    if (isUIAuthenticationEnabled() && (user != null)) {
      if ((!viewAcls.contains(user)) && (user != System.getProperty("user.name"))) {
        return false
      }
    }
    return true
  }

  def isAuthenticationEnabled(): Boolean = {
    return System.getProperty("spark.authenticate", "false").toBoolean
  }

  // user for HTTP connections
  def getHttpUser(): String = "sparkHttpUser"

  // user to use with SASL connections
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key if security is enabled, else returns null.
   */
  def getSecretKey(): String = {
    return secretKey
  }
}
