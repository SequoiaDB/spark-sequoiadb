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

import scala.collection.mutable.ArrayBuffer
import com.sequoiadb.spark.SequoiadbConfig

/**
 * Source File Name = SequoiadbPartitionerTest.scala
 * Description      = Testcase for SequoiadbPartitioner class
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20161101 Chen Zichuan           Initial Draft
 */
class SequoiadbPartitionerTest extends FlatSpec
with Matchers { 
  
  val host1 = SequoiadbHost("sdb1:11910")
  val host2 = SequoiadbHost("sdb1:11920")
  val host3 = SequoiadbHost("sdb1:11930")
  val host4 = SequoiadbHost("sdb2:11910")
  val host5 = SequoiadbHost("sdb3:11910")
  val host6 = SequoiadbHost("sdb4:11910")
  
  val hostArray1: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray1 += host1
  val hostArray2: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray2 += host2
  val hostArray3: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray3 += host3
  val hostArray4: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray4 += host4
  val hostArray5: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray5 += host5
  val hostArray6: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
  hostArray6 += host6
  
  
  val tmp_partition_list: ArrayBuffer[SequoiadbPartition] = ArrayBuffer[SequoiadbPartition]()
  
  tmp_partition_list += SequoiadbPartition ( 1, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray1,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb1', 'ServiceName':'11910'}"))
  tmp_partition_list += SequoiadbPartition ( 2, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray2,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb1', 'ServiceName':'11920'}"))
  tmp_partition_list += SequoiadbPartition ( 3, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray3,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb1', 'ServiceName':'11930'}"))               
  tmp_partition_list += SequoiadbPartition ( 4, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray4,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb2', 'ServiceName':'11910'}"))               
  tmp_partition_list += SequoiadbPartition ( 5, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray5,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb3', 'ServiceName':'11910'}"))               
  tmp_partition_list += SequoiadbPartition ( 6, SequoiadbConfig.scanTypeGetQueryMeta, 
                 hostArray6,
                 SequoiadbCollection("foo.bar"),
                 Option("{'Datablocks':[1], 'ScanType':'tbscan', 'HostName':'sdb4', 'ServiceName':'11910'}"))               
  
  val partition_list = Option (tmp_partition_list.toArray)
  
  val seq_partition_list = SequoiadbPartitioner.seqPartitionList (partition_list)
  val seq_partition_info = SequoiadbPartitioner.getConnInfo (seq_partition_list.get)
 
  it should "be equal seq_partition_info" in {
      seq_partition_info should equal ("ArrayBuffer(sdb2:11910:1, sdb1:11910:1, sdb3:11910:1, sdb1:11930:1, sdb4:11910:1, sdb1:11920:1)")
  }
}
