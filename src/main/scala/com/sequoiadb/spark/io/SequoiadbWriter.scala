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
package com.sequoiadb.spark.io

/**
 * Source File Name = SequoiadbWriter.scala
 * Description      = Writer class for Sequoiadb data source
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150307 Tao Wang           Initial Draft
 */
import com.sequoiadb.spark.SequoiadbConfig
import com.sequoiadb.spark.SequoiadbException
import com.sequoiadb.spark.util.ConnectionUtil
import com.sequoiadb.spark.schema.SequoiadbRowConverter
import com.sequoiadb.base.SequoiadbDatasource
import com.sequoiadb.base.Sequoiadb
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.bson.BSONObject
import org.slf4j.{Logger, LoggerFactory}

/**
 * SequoiaDB writer.
 * Used for saving a bunch of sequoiadb objects
 * into specified collectionspace and collection
 *
 * @param config Configuration parameters (host,collectionspace,collection,...)
 */
class SequoiadbWriter(config: SequoiadbConfig) extends Serializable {
  
  protected var ds : Option[SequoiadbDatasource] = None
  protected var connection : Option[Sequoiadb] = None

  private var LOG: Logger = LoggerFactory.getLogger(this.getClass.getName())
    
  /**
   * Storing a bunch of SequoiaDB objects.
   *
   * @param it Iterator of SequoiaDB objects.
   */
  def save(it: Iterator[Row], schema: StructType): Unit = {
    try {
      ds = Option(new SequoiadbDatasource (
          config[List[String]](SequoiadbConfig.Host),
          config[String](SequoiadbConfig.Username),
          config[String](SequoiadbConfig.Password),
          ConnectionUtil.initConfigOptions,
          ConnectionUtil.initSequoiadbOptions ))
      // pickup a connection
      connection = Option(ds.get.getConnection)
      connection.get.setSessionAttr(
          ConnectionUtil.getPreferenceObj(config[String](SequoiadbConfig.Preference)))
      // locate collection
      val cl = connection.get.getCollectionSpace(
          config[String](SequoiadbConfig.CollectionSpace)).getCollection(
              config[String](SequoiadbConfig.Collection))
      LOG.info ("bulksize = " + config[String](SequoiadbConfig.BulkSize))
      // loop through it and perform batch insert
      // batch size is defined in SequoiadbConfig.BulkSize
      val list : ArrayList[BSONObject] = new ArrayList[BSONObject]()
      while ( it.hasNext ) {
        val record = it.next
        val bsonrecord = SequoiadbRowConverter.rowAsDBObject ( record, schema )
        list.add(bsonrecord)
        if ( list.size >= config[String](SequoiadbConfig.BulkSize).toInt ) {
          cl.bulkInsert ( list, 0 )
          list.clear
        }
      }
      // insert rest of the record if there's any
      if ( list.size > 0 ) {
        cl.bulkInsert ( list, 0 )
        list.clear
      }
    } catch {
      case ex: Exception => throw SequoiadbException(ex.getMessage, ex)
    } finally {
      ds.fold(ifEmpty=()) { connectionpool =>
        connection.fold(ifEmpty=()) { conn =>
          connectionpool.close(conn)
        }
        connectionpool.close
      } // ds.fold(ifEmpty=())
    } // finally
  } // def save(it: Iterator[BSONObject]): Unit =
}


