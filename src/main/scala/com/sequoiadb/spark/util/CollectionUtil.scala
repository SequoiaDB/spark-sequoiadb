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
 * Source File Name = CollectionUtil.scala
 * Description      = Sequoiadb Collections utilities
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150307 Tao Wang           Initial Draft
 */
import com.sequoiadb.base.SequoiadbDatasource
import com.sequoiadb.base.Sequoiadb
import com.sequoiadb.spark.SequoiadbException
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode
import com.sequoiadb.exception.BaseException
import com.sequoiadb.spark.SequoiadbConfig

object CollectionUtil {
  /**
   * Check whether collection exist, and create the collection if it doesn't exist
   * Return true if not exist, return false if exist
   * @param connection Sequoiadb Connection object
   * @param collectionspace Collection space name
   * @param colleciton Collection name
   */
  private def errorIfExist (
      connection: Option[Sequoiadb],
      collectionspace: String,
      collection: String): Boolean = {
    try {
      // perform collections snapshot and check if row exist
      val snapcur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_COLLECTIONS,
          "{Name:\"" + collectionspace + "." + collection + "\"}",
          null,
          null )
      // check if there's any record available
      if ( snapcur.hasNext ) {
        snapcur.close()
        // return false if collection exist
        false
      }
      else {
        // create the collection since it's not available
        createIfNotExist ( connection, collectionspace, collection )
      }
    }
    catch {
      case ex: Exception => throw SequoiadbException ( ex.getMessage, ex )
    }
  }
  /**
   * Create the collection if not exist
   * Always return true
   * @param connection Sequoiadb Connection object
   * @param collectionspace Collection space name
   * @param colleciton Collection name
   */
  private def createIfNotExist (
      connection: Option[Sequoiadb],
      collectionspace: String,
      collection: String): Boolean = {
    try {
      // perform catalog snapshot and check if cs exist
      val cscur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_COLLECTIONSPACES,
          "{Name:\"" + collectionspace  + "\"}",
          null,
          null )
      // check if there's any record available
      if ( cscur.hasNext ) {
        cscur.close()
      }
      else {
        // create collection space
        connection.get.createCollectionSpace(collectionspace)
      }
      // perform catalog snapshot and check if collection exist
      val clcur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_COLLECTIONS,
          "{Name:\"" + collectionspace + "." + collection + "\"}",
          null,
          null )
      // check if there's any record available
      if ( clcur.hasNext ) {
        clcur.close()
      }
      else {
        // get collection space
        val cs = connection.get.getCollectionSpace(collectionspace)
        // create collection
        cs.createCollection ( collection )
      }
      true
    }
    catch {
      case ex: Exception => throw SequoiadbException ( ex.getMessage, ex )
    }
  }
  /**
   * Check whether a collection is writable for the given mode
   * Return true for writable, return false for non-writable, throw exception for error
   * @param mode SaveMode provided by caller
   * @param parameters Configuration paramters
   */
  def checkCollectionWritable (
      mode: SaveMode,
      config: SequoiadbConfig ) : Boolean = {
    var ds : Option[SequoiadbDatasource] = None
    var connection : Option[Sequoiadb] = None
    try {
      ds = Option(new SequoiadbDatasource (
          config[List[String]](SequoiadbConfig.Host),
          config[String](SequoiadbConfig.Username),
          config[String](SequoiadbConfig.Password),
          ConnectionUtil.initConfigOptions,
          ConnectionUtil.initSequoiadbOptions ))
      // pickup a connection
      connection = Option(ds.get.getConnection)
      mode match {
        case SaveMode.Append => {
          // create a new collection if doesn't exist
          createIfNotExist (
              connection,
              config[String](SequoiadbConfig.CollectionSpace),
              config[String](SequoiadbConfig.Collection))
        }
        case SaveMode.Overwrite => {
          // overwrite can't be supported without collection truncation
          throw new BaseException("SDB_OPTION_NOT_SUPPORT")
        }
        case SaveMode.ErrorIfExists => {
          // create a new collection if doens't exist
          // throw error if it's already exist
          if ( !errorIfExist (
              connection,
              config[String](SequoiadbConfig.CollectionSpace),
              config[String](SequoiadbConfig.Collection)) ) {
            throw new BaseException ( "SDB_DMS_EXIST" )
          }
          true
        }
        case SaveMode.Ignore => false
        case _ => false
      }
    } catch {
      case ex: Exception =>
        throw SequoiadbException(ex.getMessage, ex)
    } finally {
      ds.fold(ifEmpty=()) { connectionpool =>
        connection.fold(ifEmpty=()) { conn =>
          connectionpool.close(conn)
        }
        // TODO: Close connection pool
      } // ds.fold(ifEmpty=())
    } // finally
  }
}