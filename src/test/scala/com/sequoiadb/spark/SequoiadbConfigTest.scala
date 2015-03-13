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
package com.sequoiadb.spark

import org.scalatest.{FlatSpec, Matchers}
import collection.mutable.Stack

/**
 * Source File Name = SequoiadbConfigTest.scala
 * Description      = Testcase for SequoiadbConfig class
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150309 Tao Wang           Initial Draft
 */
class SequoiadbConfigTest extends FlatSpec
with Matchers {
    private val host: String = "localhost"
    private val port: Int = 12345
    private val collectionspace: String = "foo"
    private val collection: String = "bar"
    private val username : String = "testusername"
    private val password : String = "testpassword"
    private val preference : String = "{PreferedInstance:\"M\"}"
    
    "Config" should "be initialized successful" in {
      val testConfig = SequoiadbConfigBuilder()
      .set(SequoiadbConfig.Host,List(host + ":" + port))
      .set(SequoiadbConfig.CollectionSpace,collectionspace)
      .set(SequoiadbConfig.Collection,collection)
      .set(SequoiadbConfig.SamplingRatio,1.0f)
      .set(SequoiadbConfig.Username,"")
      .set(SequoiadbConfig.Password,"")
      .set(SequoiadbConfig.Preference,"")
      .build()
    }

    it should "contains predefined values" in {
      val testConfig = SequoiadbConfigBuilder()
      .set(SequoiadbConfig.Host,List(host + ":" + port))
      .set(SequoiadbConfig.CollectionSpace,collectionspace)
      .set(SequoiadbConfig.Collection,collection)
      .set(SequoiadbConfig.SamplingRatio,1.0f)
      .set(SequoiadbConfig.Username,username)
      .set(SequoiadbConfig.Password,password)
      .set(SequoiadbConfig.Preference,"")
      .build()
      testConfig.get[List[String]](SequoiadbConfig.Host).getOrElse("") should equal (List(host + ":" + port))
      testConfig.get[String](SequoiadbConfig.CollectionSpace).getOrElse("") should equal(collectionspace)
      testConfig.get[String](SequoiadbConfig.Collection).getOrElse("") should equal(collection)
      testConfig.get[Float](SequoiadbConfig.SamplingRatio).getOrElse("") should equal(1.0)
      testConfig.get[String](SequoiadbConfig.Preference).getOrElse("") should equal("")
    }
    
    it should "fails if any arguments not defined" in {
      val testConfigBuilder = SequoiadbConfigBuilder()
      .set(SequoiadbConfig.Host,List(host + ":" + port))
      .set(SequoiadbConfig.CollectionSpace,collectionspace)
      .set(SequoiadbConfig.Collection,collection)
      .set(SequoiadbConfig.SamplingRatio,1.0f)
      .set(SequoiadbConfig.Username,"")
      .set(SequoiadbConfig.Password,"")
      a [IllegalArgumentException] should be thrownBy {
        testConfigBuilder.build()
      }
    }
}