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
package com.sequoiadb.spark.partitioner

import org.scalatest.{FlatSpec, Matchers}
import collection.mutable.Stack

/**
 * Source File Name = SequoiadbHostTest.scala
 * Description      = Testcase for SequoiadbHost class
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150309 Tao Wang           Initial Draft
 */
class SequoiadbHostTest extends FlatSpec
with Matchers {    
  "HostBuilder" should "be able to take full hostname" in {
      val hostArray = new SequoiadbHostBuilder("localhost:11810").build
      hostArray.length should equal ( 1 )
  }
  
  it should "be able to take multiple full hostname" in {
      val hostArray = new SequoiadbHostBuilder("localhost:11810,localhost:11820").build
      hostArray.length should equal ( 2 )
  }
  it should "be able to add new host" in {
      val hb = new SequoiadbHostBuilder("localhost:11810,localhost:11820")
      hb.add("localhost:11830")
      hb.build.length should equal ( 3 )
  }
  it should "be able to remove duplicate" in {
      val hb = new SequoiadbHostBuilder("localhost:11810,localhost:11820")
      hb.add("localhost:11820")
      val hostArray = hb.build
      hostArray.length should equal ( 2 )
  }
  "Host" should "be able to match each other" in {
      val host1 = SequoiadbHost("localhost:11810")
      val host2 = SequoiadbHost("localhost:11810")
      host1.equals(host2) should equal ( true )
  }
  it should "be able to find difference with others" in {
      val host1 = SequoiadbHost("localhost:11810")
      val host2 = SequoiadbHost("localhost:11820")
      host1.equals(host2) should equal ( false )
  }
}