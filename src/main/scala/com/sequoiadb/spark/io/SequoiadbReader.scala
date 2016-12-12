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
 * Source File Name = SequoiadbReader.scala
 * Description      = Reader class for Sequoiadb data source
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150307 Tao Wang           Initial Draft
 */
import com.sequoiadb.spark.SequoiadbConfig
import com.sequoiadb.spark.partitioner.SequoiadbPartition
import com.sequoiadb.spark.SequoiadbException
import com.sequoiadb.spark.util.ConnectionUtil
import org.apache.spark.Partition
import org.apache.spark.sql.sources._
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList
import com.sequoiadb.base.DBCursor
import com.sequoiadb.base.SequoiadbDatasource
import com.sequoiadb.base.Sequoiadb
import scala.collection.JavaConversions._
import org.slf4j.{Logger, LoggerFactory}
//import java.io.FileOutputStream  
import org.bson.util.JSON
import java.util.regex.Pattern
import org.bson.types.BSONDecimal

/**
 *
 * @param config Configuration object.
 * @param requiredColumns Pruning fields
 * @param filters Added query filters
 */
class SequoiadbReader(
  config: SequoiadbConfig,
  requiredColumns: Array[String],
  filters: Array[Filter]) {

  
  private var LOG: Logger = LoggerFactory.getLogger(this.getClass.getName())
  private var dbConnectionPool : Option[SequoiadbDatasource] = None
  private var dbConnection : Option[Sequoiadb] = None
  private var dbCursor : Option[DBCursor] = None
  def close(): Unit = {
    dbCursor.fold(ifEmpty=()) { cursor=>
      cursor.close()
      dbCursor = None
    }
    dbConnectionPool.fold(ifEmpty=()) { connectionpool =>
      dbConnection.fold(ifEmpty=()) { connection =>
        connection.closeAllCursors()
        connectionpool.close(connection)
      }
      connectionpool.close
    }
  }

  def hasNext: Boolean = {
    dbCursor.fold(ifEmpty=false){_.hasNext()}
  }

  def next(): BSONObject = {
    dbCursor.fold(ifEmpty=
      throw new IllegalStateException("dbCursor is not initialized"))(_.getNext)
  }


  /**
   * Initialize SequoiaDB reader
   * @param partition Where to read from
   */
  def init(partition: Partition): Unit = {
//    var out: FileOutputStream = new FileOutputStream ("/root/software/spark-2.0-hadoop2.6/logs/test.txt");;
//    out.write ("enter SequoiadbReader.init function\n".getBytes)
    // convert from Spark Partition to SequoiadbPartition
    val sdbPartition = partition.asInstanceOf[SequoiadbPartition]
    try {
      // For now let's simply turn the selection to sdbdatasource
      dbConnectionPool = Option ( new SequoiadbDatasource (
          // use the hosts for the given partition
          sdbPartition.hosts.map{it=>it.toString},
          config[String](SequoiadbConfig.Username),
          config[String](SequoiadbConfig.Password),
          ConnectionUtil.initConfigOptions,
          ConnectionUtil.initSequoiadbOptions ) )
      // pickup a connection
      val connection = dbConnectionPool.get.getConnection
      
      connection.setSessionAttr(
          ConnectionUtil.getPreferenceObj(config[String](SequoiadbConfig.Preference)))
      // get collection space
      val cs = connection.getCollectionSpace(sdbPartition.collection.collectionspace)
      // get collection
      val cl = cs.getCollection(sdbPartition.collection.collection)
      if (sdbPartition.scanType.equals(SequoiadbConfig.scanTypeGetQueryMeta)){
        var _metaObjStr: String = null
        var _metaObj: BSONObject = null
        if (sdbPartition.metaObjStr != None){
          _metaObjStr = sdbPartition.metaObjStr.get
          _metaObj = JSON.parse (_metaObjStr).asInstanceOf[BSONObject]
        } 
        val metaObj = new BasicBSONObject()
        metaObj.put ("$Meta", _metaObj)
        
        // perform query by dataBlocks
        dbCursor = Option(cl.query (
            SequoiadbReader.queryPartition(filters),
            SequoiadbReader.selectFields(requiredColumns),
            null,
            metaObj))

      }
      else if (sdbPartition.scanType.equals(SequoiadbConfig.scanTypeExplain)){
        // perform query by subCollection
        dbCursor = Option(cl.query (
            SequoiadbReader.queryPartition(filters),
            SequoiadbReader.selectFields(requiredColumns),
            null,
            null))

        
      }
      else {
      }
//      out.close
    }
    catch {
      case ex: Exception =>
        throw SequoiadbException(ex.getMessage, ex)
    }
  }
}

object SequoiadbReader {
  
  /**
   * Create query partition using given filters.
   *
   * @param filters the Spark filters to be converted to SequoiaDB filters
   * @return the query object
   */
  def queryPartition ( filters: Array[Filter]): BSONObject = {
    
    def changeJavaMathBigDecimalType (value: Any): Any = {
      value match {
        case value: java.math.BigDecimal => new BSONDecimal (value.asInstanceOf[java.math.BigDecimal].toString())
        case _ => value
      }
    }
    val obj : BSONObject = new BasicBSONObject
    filters.foreach {
      case EqualTo(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$et", value)
        obj.put(attribute,subobj)
      }
      case GreaterThan(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$gt", value)
        obj.put(attribute,subobj)
      }
      case GreaterThanOrEqual(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$gte", value)
        obj.put(attribute,subobj)
      }
      case In(attribute, values) => {
        val subobj : BSONObject = new BasicBSONObject
        val arr : BSONObject = new BasicBSONList
        Array.tabulate(values.length){ i => arr.put(""+i, changeJavaMathBigDecimalType (values(i)))}
        subobj.put("$in", arr)
        obj.put(attribute,subobj)
      }
      case LessThan(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$lt", value)
        obj.put(attribute,subobj)
      }
      case LessThanOrEqual(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$lte", value)
        obj.put(attribute,subobj)
      }
      case IsNull(attribute) => {
        val subobj : BSONObject = new BasicBSONObject
        subobj.put("$isnull", 1)
        obj.put(attribute,subobj)
      }
      case IsNotNull(attribute) => {
        val subobj : BSONObject = new BasicBSONObject
        subobj.put("$isnull", 0)
        obj.put(attribute,subobj)
      }
      case And(left, right) => {
        val leftCond : BSONObject = queryPartition ( Array(left) )
        val rightCond : BSONObject = queryPartition ( Array(right) )
        val arr : BSONObject = new BasicBSONList
        arr.put ( "0", leftCond )
        arr.put ( "1", rightCond )
        obj.put ( "$and",arr )
      }
      case Or(left, right) => {
        val leftCond : BSONObject = queryPartition ( Array(left) )
        val rightCond : BSONObject = queryPartition ( Array(right) )
        val arr : BSONObject = new BasicBSONList
        arr.put ( "0", leftCond )
        arr.put ( "1", rightCond )
        obj.put ( "$or",arr )
      }
      case Not(child) =>{
        val notCond : BSONObject = queryPartition ( Array(child) )
        val arr : BSONObject = new BasicBSONList
        arr.put ( "0", notCond )
        obj.put ( "$not",arr )
      }
      case StringStartsWith(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        val subobj: Pattern = Pattern.compile("^" + value,Pattern.CASE_INSENSITIVE);
        obj.put (attribute,subobj)
      }
      case StringEndsWith(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        val subobj: Pattern = Pattern.compile(value + "$",Pattern.CASE_INSENSITIVE);
        obj.put (attribute,subobj)
      }
      case StringContains(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        val subobj: Pattern = Pattern.compile(".*" + value + ".*",Pattern.CASE_INSENSITIVE);
        obj.put (attribute,subobj)
      }
    }
    obj
  }
  /**
   *
   * Prepared BSONObject used to specify required fields in sequoiadb 'query'
   * @param fields Required fields
   * @return A sequoiadb object that represents selector.
   */
  def selectFields(fields: Array[String]): BSONObject = {
    val res : BSONObject = new BasicBSONObject
    fields.map { res.put ( _, null ) }
    res
  }
  
}
