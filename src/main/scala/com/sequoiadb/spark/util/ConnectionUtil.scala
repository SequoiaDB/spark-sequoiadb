/*
 *  Licensed to SequoiaDB (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The SequoiaDB (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.sequoiadb.spark.util

/**
 * Source File Name = ConnectionUtil.scala
 * Description      = Connection utilities
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150307 Tao Wang           Initial Draft
 */
import com.sequoiadb.datasource.DatasourceOptions
import com.sequoiadb.net.ConfigOptions
import com.sequoiadb.exception.BaseException
import org.bson.util.JSON
import org.bson.BSONObject

object ConnectionUtil {
  def initConfigOptions : ConfigOptions = {
    val nwOpt = new ConfigOptions()
    // 5 seconds timeout
    nwOpt.setConnectTimeout(5000)
    // do not retry
    nwOpt.setMaxAutoConnectRetryTime(0)
    nwOpt
  }
  def initSequoiadbOptions : DatasourceOptions = {
    val dsOpt = new DatasourceOptions()
    // connection pool max to 3
    dsOpt.setMaxCount (3)
    dsOpt.setDeltaIncCount (1)
    dsOpt.setMaxIdleCount (1)
    dsOpt
  }
  def getPreferenceObj ( preference: String ) : BSONObject = {
    try {
      JSON.parse ( preference ).asInstanceOf[BSONObject]
    } catch {
      case ex: Exception => throw new BaseException ( "SDB_INVALIDARG" )
    }
  }
}
