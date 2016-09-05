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

/**
 * Source File Name = SequoiadbPartition.scala
 * Description      = Partition unit for SequoiaDB
 * When/how to use  = SequoiadbPartition is used by RDD to get partition information
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */
import org.apache.spark.Partition
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

/**
 * A SequoiaDB Partition is a minimum unit of repeatable-read operation
 * In the current release each SequoiadbPartition represent a shard
 * in the cluster, that means whenever a partition operation failed during the middle
 * it's always safe to restart from scratch on a different node
 * @param index Partition index
 * @param hosts Hosts that hold partition data
 * @param collection The collection need to read for such partition
 * @param metaObjStr When query by table scan, will be used
 */
case class SequoiadbPartition(
  var index: Int,
  scanType: Int,
  hosts: Seq[SequoiadbHost],
  collection: SequoiadbCollection,
  metaObjStr: Option[String]) extends Partition