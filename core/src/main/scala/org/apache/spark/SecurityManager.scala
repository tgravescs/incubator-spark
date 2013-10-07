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

/** 
 * Spark class responsible for security.  
 */
private object SecurityManager extends Logging {

  // default to auth off
  private var authOn : Boolean = System.getProperty("spark.authenticate", "false").toBoolean
  private val authUIOn : Boolean = System.getProperty("spark.ui.authenticate", "false").toBoolean
  // list of users allowed to view the UI
  private val viewAcls = System.getProperty("spark.ui.view.acls", "").split(',').map(_.trim()).toSet

  def isUIAuthenticationEnabled() : Boolean = {
    return authUIOn
  }

  // allow anyone in the acl list and the application owner 
  def checkUIViewPermissions(user : String) : Boolean = {
    if (isUIAuthenticationEnabled() && (user != null)) {
      if ((!viewAcls.contains(user)) && (user != System.getProperty("user.name"))) {
        return false
      }
    }
    return true
  }

  def isAuthenticationEnabled() : Boolean = {
    return authOn
  }

  // here for testing
  private[spark] def setAuthenticationOn(auth: Boolean) = {
    authOn = auth
  }

  // user for HTTP connections
  def getHttpUser() : String = {
    return "sparkHttpUser"
  }

  // user to use with SASL connections
  def getSaslUser() : String = {
    return "sparkSaslUser"
  }

  /**
   * Gets the secret key if security is enabled, else returns null.
   * In Yarn mode its uses Hadoop UGI to pass the secret as that
   * will keep it protected.  For a standalone SPARK cluster
   * use a environment variable SPARK_SECRET to specify the secret.
   * This probably isn't ideal but only the user who starts the process
   * should have access to view the variable (atleast on Linux).
   */
  def getSecretKey() : String = {
    if (authOn) {
      if (java.lang.Boolean.valueOf(System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))) {
        val credentials = UserGroupInformation.getCurrentUser().getCredentials()
        val secretKey = credentials.getSecretKey(new Text("akkaCookie"))
        if (secretKey != null) {
          logDebug("in yarn mode")
          return new Text(secretKey).toString
        } else {
          logDebug("in yarn mode, secret key is null")
        }
        return null
      } else {
        // java property used for testing - env variable should be used normally
        val secret = System.getProperty("SPARK_SECRET", System.getenv("SPARK_SECRET")) 
        if (secret != null && !secret.isEmpty()) {
          logDebug("in getSecretKey")
          return secret
        } else {
          logDebug("in getSecretKey , secret key is null")
        }
        return null
      }
    } else {
      logDebug("auth off")
    }
    return null
  }
}
