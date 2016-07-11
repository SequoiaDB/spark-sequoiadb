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
 * Source File Name = SequoiadbFunctions.scala
 * Description      = SequoiaDB Function definition
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */

import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.language.implicitConversions
import com.sequoiadb.spark.io.SequoiadbWriter

/**
 * @param sqlContext Spark SQLContext
 */
class SequoiadbContext(sqlContext: SQLContext) {

  /**
   * It retrieves a bunch of SequoiaDB objects
   * given a SequoiaDB configuration object.
   * @param config SequoiaDB configuration object
   * @return A schemaRDD
   */
  def fromSequoiadb(config: SequoiadbConfig): DataFrame =
    sqlContext.baseRelationToDataFrame(
      SequoiadbRelation(config, None)(sqlContext))
      
}

/**
 * @param schemaRDD Spark SchemaRDD
 */
class SequoiadbDataFrame(dataframe: DataFrame) extends Serializable {
  /**
   * It allows storing data in SequoiaDB from some existing SchemaRDD
   * @param config SequoiaDB configuration object
   * @param batch It indicates whether it has to be saved in batch mode or not.
   */
  def saveToSequoiadb(config: SequoiadbConfig): Unit = {
    val schema = dataframe.schema
    dataframe.foreachPartition(it => {
      new SequoiadbWriter(config).save(it, schema)
    })
  }
}

/**
 *  Helpers for getting / storing SequoiaDB data.
 */
trait SequoiadbFunctions {

  implicit def toSequoiadbContext(sqlContext: SQLContext): SequoiadbContext =
    new SequoiadbContext(sqlContext)

  implicit def toSequoiadbSchemaRDD(dataframe: DataFrame): SequoiadbDataFrame =
    new SequoiadbDataFrame(dataframe)

}
