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
import com.sequoiadb.base.DBQuery
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
 * @param query return data type, 0 = data in bson, 1 = data in csv
 * @param query limit number, default -1 (query all data)
 */
class SequoiadbReader(
  config: SequoiadbConfig,
  requiredColumns: Array[String],
  filters: Array[Filter],
  queryReturnType: Int = SequoiadbConfig.QUERYRETURNBSON,
  queryLimit: Long = -1) {

  
  
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
//    var out: FileOutputStream = new FileOutputStream ("/root/software/spark-2.0-hadoop2.6/logs/test.txt", true);
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
            metaObj,
            0,
            queryLimit,
            queryReturnType))

      }
      else if (sdbPartition.scanType.equals(SequoiadbConfig.scanTypeExplain)){
        // perform query by subCollection
        dbCursor = Option(cl.query (
            SequoiadbReader.queryPartition(filters),
            SequoiadbReader.selectFields(requiredColumns),
            null,
            null,
            0,
            queryLimit,
            queryReturnType))

        
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

class SequoiadbFilter extends  Filter {
  
}


object SequoiadbReader {
  private var LOG: Logger = LoggerFactory.getLogger(this.getClass.getName())
  
  /**
   * Create query partition using given filters.
   *
   * @param filters the Spark filters to be converted to SequoiaDB filters
   * @return the query object
   */
  def queryPartition ( filters: Array[Filter]): BSONObject = {
    
    /*
     * when value's data type = BigDecimal, then filter in BSON will chose BSONDecimal
     */
    def changeJavaMathBigDecimalType (value: Any): Any = {
      value match {
        case value: java.math.BigDecimal => new BSONDecimal (value.asInstanceOf[java.math.BigDecimal].toString())
        case _ => value
      }
    }
    
    /*
     * init filter in BSON, but do not include OR and AND filters
     */
    def initFilterObj (obj: BSONObject, attribute: String, subobj: Any): BSONObject = {
      val retrunObj : BSONObject = new BasicBSONObject ()
      LOG.info ("enter initFilterObj, obj = " + obj.toString)
      if (obj.containsField("$and")) {
        val tmpArr = obj.get ("$and").asInstanceOf [BasicBSONList]
        val tmpObj : BSONObject = new BasicBSONObject ()
        tmpObj.put (attribute, subobj)
        tmpArr.add (tmpObj)
                
        obj.put ("$and", tmpArr)
        retrunObj.putAll(obj)
      } else if (obj.containsField(attribute)){
        val tmpArr = new BasicBSONList ()
        val leftCond = obj
        val rightCond = new BasicBSONObject ()
        
        rightCond.put (attribute, subobj)
        
        tmpArr.add (leftCond)
        tmpArr.add (rightCond)
        retrunObj.put ("$and", tmpArr)
      } else {
        obj.put (attribute, subobj)
        retrunObj.putAll(obj)
      }
      
      retrunObj
    }
    
    /*
     * init filter in BSON, but just deal with AND filters
     */
    def initFilterObjForAND (left: BSONObject, right: BSONObject): BSONObject = {
      var obj : BSONObject = new BasicBSONObject ()
      if (left.containsField("$and") && right.containsField("$and")) {
        val tmpArr = new BasicBSONList()
        left.get ("$and").asInstanceOf [BasicBSONList].foreach {
          case value:BSONObject => {
            tmpArr.add (value)
          }
        }
        
        right.get ("$and").asInstanceOf [BasicBSONList].foreach {
          case value:BSONObject => {
            tmpArr.add (value)
          }
        }
        obj.put ("$and", tmpArr)
      }
      else if (left.containsField("$or") || right.containsField("$or")){
        val tmpArr = new BasicBSONList()
        tmpArr.add (left)
        tmpArr.add (right)
        obj.put ("$and", tmpArr)
      }
      else if (left.containsField("$and") && !right.containsField("$and")){
        var tmpObj : BSONObject = new BasicBSONObject ()
        for (key <- right.keySet().toArray()) {
          tmpObj = initFilterObj (left, 
                                  key.asInstanceOf [String], 
                                  right.get (key.asInstanceOf [String])
                                  )
        }
        obj = tmpObj
      }
      else if (!left.containsField("$and") && right.containsField("$and")){
        var tmpObj : BSONObject = new BasicBSONObject ()
        for (key <- left.keySet().toArray()) {
          tmpObj = initFilterObj (right, 
                                  key.asInstanceOf [String], 
                                  left.get (key.asInstanceOf [String])
                                  )
        }
        obj = tmpObj
      }
      else {
        var tmpObj : BSONObject = new BasicBSONObject ()
        for (key <- right.keySet().toArray()) {
          tmpObj = initFilterObj (left, 
                                  key.asInstanceOf [String], 
                                  right.get (key.asInstanceOf [String])
                                  )
        }
        obj = tmpObj
      }

      obj
    }
    
    /*
     * init filter in BSON, but just deal with OR filters
     */
    def initFilterObjForOR (left: BSONObject, right: BSONObject): BSONObject = {
      var obj : BSONObject = new BasicBSONObject ()
      
      if (left.containsField("$or") && right.containsField("$or")) {
        val tmpArr = new BasicBSONList()
        left.get ("$or").asInstanceOf [BasicBSONList].foreach {
          case value:BSONObject => {
            tmpArr.add (value)
          }
        }
        
        right.get ("$or").asInstanceOf [BasicBSONList].foreach {
          case value:BSONObject => {
            tmpArr.add (value)
          }
        }
        obj.put ("$or", tmpArr)
      }
      else if (left.containsField("$or") && !right.containsField("$or")) {
        val tmpArr = left.get ("$or").asInstanceOf [BasicBSONList]
        tmpArr.add (right)
        obj = left
        obj.put ("$or", tmpArr)
      }
      else if (!left.containsField("$or") && right.containsField("$or")) {
        val tmpArr = right.get ("$or").asInstanceOf [BasicBSONList]
        tmpArr.add (left)
        obj = right
        obj.put ("$or", tmpArr)
      }
      else {
        val tmpArr = new BasicBSONList()
        tmpArr.add (left)
        tmpArr.add (right)
        
        obj.put ("$or", tmpArr)
      }
      
      obj
    }
    
    var obj : BSONObject = new BasicBSONObject
    filters.foreach {
      case EqualTo(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$et", value)
        obj = initFilterObj (obj, attribute, subobj)
        
        LOG.info ("et " + obj.toString)
      }
      case GreaterThan(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$gt", value)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("gt " + obj.toString)
      }
      case GreaterThanOrEqual(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$gte", value)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("gte " + obj.toString)
      }
      case In(attribute, values) => {
        val subobj : BSONObject = new BasicBSONObject
        val arr : BSONObject = new BasicBSONList
        Array.tabulate(values.length){ i => arr.put(""+i, changeJavaMathBigDecimalType (values(i)))}
        subobj.put("$in", arr)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("in " + obj.toString)
      }
      case LessThan(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$lt", value)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("lt " + obj.toString)
      }
      case LessThanOrEqual(attribute, _value) => {
        val subobj : BSONObject = new BasicBSONObject
        val value = changeJavaMathBigDecimalType (_value)
        subobj.put("$lte", value)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("lte " + obj.toString)
      }
      case IsNull(attribute) => {
        val subobj : BSONObject = new BasicBSONObject
        subobj.put("$isnull", 1)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("isnull " + obj.toString)
      }
      case IsNotNull(attribute) => {
        val subobj : BSONObject = new BasicBSONObject
        subobj.put("$isnull", 0)
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("isnotnull " + obj.toString)
      }
      case And(left, right) => {
        val leftCond : BSONObject = queryPartition ( Array(left) )
        val rightCond : BSONObject = queryPartition ( Array(right) )
        obj = initFilterObjForAND (leftCond, rightCond)
        LOG.info ("and " + obj.toString)
      }
      case Or(left, right) => {
        val leftCond : BSONObject = queryPartition ( Array(left) )
        val rightCond : BSONObject = queryPartition ( Array(right) )
        val arr : BSONObject = new BasicBSONList
        obj = initFilterObjForOR (leftCond, rightCond)
        LOG.info ("or " + obj.toString)
      }
      case Not(child) =>{
        val notCond : BSONObject = queryPartition ( Array(child) )
        val arr : BSONObject = new BasicBSONList
        arr.put ( "0", notCond )
        obj.put ( "$not",arr )
        LOG.info ("not " + obj.toString)
      }
      case StringStartsWith(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        // do not set options
        val subobj: Pattern = Pattern.compile("^" + value + ".*");
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("startwith " + obj.toString)
      }
      case StringEndsWith(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        // do not set options
        val subobj: Pattern = Pattern.compile(".*" + value + "$");
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("endwith " + obj.toString)
      }
      case StringContains(attribute, _value) =>{
        val value = changeJavaMathBigDecimalType (_value)
        // do not set options
        val subobj: Pattern = Pattern.compile(".*" + value + ".*");
        obj = initFilterObj (obj, attribute, subobj)
        LOG.info ("contains " + obj.toString)
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
