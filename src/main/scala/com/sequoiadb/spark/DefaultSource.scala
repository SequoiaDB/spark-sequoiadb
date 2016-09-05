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

/**
 * Source File Name = DefaultSource.scala
 * Description      = DefaultSource for com.sequoiadb.spark package
 * When/how to use  = Default source is loaded by Spark
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */

import org.apache.spark.sql.sources._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import SequoiadbConfig._
import com.sequoiadb.exception._
import com.sequoiadb.spark.io.SequoiadbWriter
import com.sequoiadb.spark.util.CollectionUtil

/**
 * Allows creation of SequoiaDB based tables using
 * the syntax CREATE TEMPORARY TABLE ... USING com.sequoiadb.spark
 * Required options are detailed in [[com.sequoiadb.spark.SequoiadbConfig]]
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  /**
   * Create relation without providing schema
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }
  
  /**
   * Create SequoiadbConfig based on input parameters
   */
  private def createConfig ( parameters: Map[String, String] ): SequoiadbConfig = {
    /** We will assume hosts are provided like 'host:port,host2:port2,...'*/
    val host = parameters
      .getOrElse(Host, notFound[String](Host))
      .split(",").toList

    val collectionspace = parameters.getOrElse(CollectionSpace, notFound(CollectionSpace))

    val collection = parameters.getOrElse(Collection, notFound(Collection))

    val samplingRatio = parameters
      .get(SamplingRatio)
      .map(_.toDouble).getOrElse(DefaultSamplingRatio)
      
    val username = parameters.getOrElse(Username, DefaultUsername)
    
    val password = parameters.getOrElse(Password, DefaultPassword)
    
    val preference = parameters.getOrElse(Preference, DefaultPreference)
    
    val scanType = parameters.getOrElse(ScanType, DefaultScanType)

    SequoiadbConfigBuilder()
        .set(Host,host)
        .set(CollectionSpace,collectionspace)
        .set(Collection,collection)
        .set(SamplingRatio,samplingRatio)
        .set(Username,username)
        .set(Password,password)
        .set(Preference,preference)
        .set(ScanType, scanType).build()
  }
  
  /**
   * Create relation and providing schema
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType ): BaseRelation = {
    SequoiadbRelation(createConfig(parameters),Option(schema))(sqlContext)

  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val config: SequoiadbConfig = createConfig ( parameters )
    try {
      // check if the collection exist or writable
      if ( CollectionUtil.checkCollectionWritable ( mode, config) ) {
        // if it return true, that means we should write the data into collection
        data.foreachPartition(it => {
          // always write through coord node which specified in config
          new SequoiadbWriter(config).save(it, data.schema)
            //it.map(row =>SequoiadbRowConverter.rowAsDBObject(row, data.schema)))
        })
      }
      // create relation either returns true or false
      createRelation(sqlContext, parameters, data.schema)
    } catch {
      // convert sequoiadb exception to runtime exception
      case ex: BaseException => sys.error(s"Error:" + ex.getErrorCode + "(" + ex.getMessage + ")")
      case ex: Exception => sys.error(s"Error:" + ex.getMessage )
    }
  }
}
