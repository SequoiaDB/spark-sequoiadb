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
 * Source File Name = SequoiadbPartitioner.scala
 * Description      = Utiliy to extract partition information for a given collection
 * When/how to use  = SequoiadbRDD.getPartitions calls this class to extract Partition array
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */
import com.sequoiadb.spark.SequoiadbConfig
import com.sequoiadb.spark.SequoiadbException
import com.sequoiadb.spark.schema.SequoiadbRowConverter
import com.sequoiadb.spark.util.ConnectionUtil
import com.sequoiadb.base.SequoiadbDatasource
import com.sequoiadb.base.Sequoiadb
import com.sequoiadb.exception.BaseException
import com.sequoiadb.exception.SDBErrorLookup
import org.bson.BSONObject
import org.bson.types.BasicBSONList
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.sql.sources.Filter

/**
 * @param config Partition configuration
 */
class SequoiadbPartitioner(
  config: SequoiadbConfig) extends Serializable {
  
  /**
   * build full collection name from cs and cl name
   */
  val collectionname: String = config[String](SequoiadbConfig.CollectionSpace) + "." +
                                config[String](SequoiadbConfig.Collection)
  
  /**
   * Get database replication group information
   * @param connection SequoiaDB connection object
   */
  private def getReplicaGroup ( connection: Sequoiadb ) : 
    Option[ArrayBuffer[Map[String,AnyRef]]] = {
    val rg_list : ArrayBuffer[Map[String, AnyRef]] = new ArrayBuffer[Map[String, AnyRef]]
    // try to get replica group, if SDB_RTN_COORD_ONLY is received, we create
    // partition for standalone only
    try {
      // go through all replica groups
      val rgcur = connection.listReplicaGroups
      // append each group into rg list
      while ( rgcur.hasNext ) {
        rg_list.append(SequoiadbRowConverter.dbObjectToMap(rgcur.getNext()))
      }
      rgcur.close
      Option(rg_list)
    }
    catch{
      // if we get SDB_RTN_COORD_ONLY error, that means we connect to standalone or data node
      // in that case we simply return null instead of throwing exception
      case ex: BaseException => {
        if ( ex.getErrorCode == SDBErrorLookup.getErrorCodeByType("SDB_RTN_COORD_ONLY")) {
          None
        }
        else {
          throw SequoiadbException(ex.getMessage, ex)
        }
      }
      case ex: Exception => throw SequoiadbException(ex.getMessage,ex)
    }
  }
  
  /**
   * Determine whether a given collection is main collection or regular collection
   * @param connection SequoiaDB connection object
   */
  private def isMainCollection ( connection: Option[Sequoiadb] ): Boolean = {
    try {
      // perform catalog snapshot and extract IsMainCL attribute
      // if the result does not include such field or it's false, that means normal collection
      val snapcur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_CATALOG,
          "{Name:\"" + collectionname + "\"}",
          "{IsMainCL:false}",
          null )
      // we attempt to pick the first element in result set since collection name is unique
      if ( snapcur.hasNext ) {
        // convert to map
        val colinfo = SequoiadbRowConverter.dbObjectToMap(snapcur.getNext)
        // close cursor
        snapcur.close
        colinfo("IsMainCL").asInstanceOf[Boolean]
      }
      else {
        // if we cannot find anything by snapshot, that means the collection doesn't exist
        val ex = new BaseException ( "SDB_DMS_NOTEXIST" )
        throw SequoiadbException ( ex.getMessage, ex )
      }
    }
    catch {
      case ex: Exception => throw SequoiadbException ( ex.getMessage, ex )
    }
  }
  
  /**
   * Extract child collections from main collection
   * @param connection SequoiaDB connection object
   */
  private def getChildForMainCollection (
       connection: Option[Sequoiadb] ): Option[ArrayBuffer[SequoiadbCollection]] = {
    val ab: ArrayBuffer[SequoiadbCollection] = new ArrayBuffer[SequoiadbCollection]()
    try {
      // perform catalog snapshot and extract CataInfo field
      val snapcur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_CATALOG,
          "{Name:\"" + collectionname + "\"}",
          "{CataInfo:1}",
          null )
      // for each record, we get CataInfo.SubCLName
      if ( snapcur.hasNext ) {
        val colinfo = SequoiadbRowConverter.dbObjectToMap(snapcur.getNext)
        // close cursor
        snapcur.close
        // Make sure it's array, otherwise ignore
        if ( colinfo("CataInfo").isInstanceOf[BasicBSONList] ) {
          // for each element in array, we pickup SubCLName field and append to ab
          colinfo("CataInfo").asInstanceOf[BasicBSONList].toMap.map {
            case ( k,v )=>(
                ab.append ( SequoiadbCollection ( SequoiadbRowConverter.dbObjectToMap(
                    v.asInstanceOf[BSONObject])("SubCLName").asInstanceOf[String])))
          }
        }
      }
      else {
        // if there's nothing returned by snapshot, it means the collection doesn't exist
        val ex = new BaseException ( "SDB_DMS_NOTEXIST" )
        throw SequoiadbException ( ex.getMessage, ex )
      }
      Option(ab)
    }
    catch {
      case ex: Exception => throw SequoiadbException ( ex.getMessage, ex )
    }
  }
  
  /**
   * Convert rg_list to replication group map
   * @param rg_list replication group list
   */
  private def makeRGMap ( rg_list: ArrayBuffer[Map[String, AnyRef]] ) :
    HashMap[String, Seq[SequoiadbHost]] = {
    val rg_map : HashMap[String, Seq[SequoiadbHost]] = HashMap[String, Seq[SequoiadbHost]]()
    // for each record from listReplicaGroup result
    for ( rg <- rg_list ) {
      val hostArray: ArrayBuffer[SequoiadbHost] = ArrayBuffer[SequoiadbHost]()
      // pickup "Group" field of Array and loop for each element
      for ( node <- SequoiadbRowConverter.dbObjectToMap(
          rg("Group").asInstanceOf[BasicBSONList]) ) {
        // pickup the Service name from group member and check each element's type
        for ( port <- SequoiadbRowConverter.dbObjectToMap(
            node._2.asInstanceOf[BSONObject].get("Service").asInstanceOf[BasicBSONList]) ) {
          // if the type represents direct connect port, we record it in hostArray
          if ( port._2.asInstanceOf[BSONObject].get("Type").asInstanceOf[Int] == 0 ) {
            // let's record the hostname and port name
            hostArray.append ( SequoiadbHost (
                node._2.asInstanceOf[BSONObject].get("HostName").asInstanceOf[String] + ":" +
                port._2.asInstanceOf[BSONObject].get("Name").asInstanceOf[String] ))
          }
        }
      }
      // build the map based on the group name and array we built
      rg_map += ( rg("GroupName").asInstanceOf[String] -> hostArray )
    }
    rg_map
  }
  
  /**
   * Get all group names associated with a collection
   * @param connection SequoiaDB connection object
   * @param collection Collection name, note it could be either subCL or normal CL
   */
  private def getCollectionGroupName (
      connection: Option[Sequoiadb], 
      collection: SequoiadbCollection ): Array[String] = {
    val ab: ArrayBuffer[String] = new ArrayBuffer[String]()
    try {
      // perform collection snapshot and pickup Details field
      val snapcur = connection.get.getSnapshot ( Sequoiadb.SDB_SNAP_COLLECTIONS,
          "{Name:\"" + collection.collectionname + "\"}",
          "{Details:1}",
          null )
      if ( snapcur.hasNext ) {
        val colinfo = SequoiadbRowConverter.dbObjectToMap(snapcur.getNext)
        // close cursor
        snapcur.close
        // get details field and make sure it's array
        if ( colinfo("Details").isInstanceOf[BasicBSONList] ) {
          // for each element in array, pickup GroupName field and append to result
          colinfo("Details").asInstanceOf[BasicBSONList].toMap.map {
            case ( k,v )=>(
                ab.append ( v.asInstanceOf[BSONObject].get("GroupName").asInstanceOf[String]))
          }
        }
      }
      else {
        // if snapshot doesn't give any output, that means collection doesn't exist
        val ex = new BaseException ( "SDB_DMS_NOTEXIST" )
        throw SequoiadbException ( ex.getMessage, ex )
      }
      ab.toArray
    }
    catch {
      case ex: Exception => throw SequoiadbException ( ex.getMessage, ex )
    }
  }

  /**
   * Extract partition information for given collection
   * TODO: Use query explain based on filters to get real necessary shards
   */
  def computePartitions(filters: Array[Filter]): Array[SequoiadbPartition] = {
    var ds : Option[SequoiadbDatasource] = None
    var connection : Option[Sequoiadb] = None
    val partition_list: ArrayBuffer[SequoiadbPartition] = ArrayBuffer[SequoiadbPartition]()
    var rg_list : Option[ArrayBuffer[Map[String, AnyRef]]] = None
    var rg_map : HashMap[String, Seq[SequoiadbHost]] = HashMap[String, Seq[SequoiadbHost]]()
    try {
      // TODO: need to close ds afterwards
      ds = Option(new SequoiadbDatasource (
          config[List[String]](SequoiadbConfig.Host),
          //new ArrayList(Arrays.asList(config[List[String]](SequoiadbConfig.Host).toArray: _*)),
          config[String](SequoiadbConfig.Username),
          config[String](SequoiadbConfig.Password),
          ConnectionUtil.initConfigOptions,
          ConnectionUtil.initSequoiadbOptions ))
      // pickup a connection
      connection = Option(ds.get.getConnection)
      connection.get.setSessionAttr(
          ConnectionUtil.getPreferenceObj(config[String](SequoiadbConfig.Preference)))
      // get replica group
      rg_list = getReplicaGroup ( connection.get )
      // if there's no replica group can be found, let's return the current connected node
      // as the only partition
      if ( rg_list == None ){
        return Array[SequoiadbPartition] ( SequoiadbPartition ( 0,
          Seq[SequoiadbHost] (SequoiadbHost(connection.get.getServerAddress.getHost + ":" +
            connection.get.getServerAddress.getPort )),
            SequoiadbCollection(collectionname)))
      }
      else {
        // we now need to extract rg list and form a map
        // each key in map represent a group name, with one or more hosts associated with a group
        rg_map = makeRGMap(rg_list.get)
  
        // now we have connection and we know it's cluster setup
        // collections is a list of collection or subcollections we need to query
        var collections: Option[ArrayBuffer[SequoiadbCollection]] = None
        // determine if it's main collection or normal
        if ( isMainCollection ( connection )) {
          // for main collection, we need to append collections with all subcollections
          // TODO: optimization can be done here in order to filter out
          // unnecessary subcollections
          collections = getChildForMainCollection ( connection )
        }
        else {
          // if it's normal collection, just append to collection array
          collections = Option ( ArrayBuffer[SequoiadbCollection](
              SequoiadbCollection(collectionname) ) )
        }
        
        // now let's loop through each collection and make out the partitions
        var partition_id = 0
        for ( collection <- collections.get.toArray ) {
          // extract group information for the given sub collection or normal cl
          val group_list = getCollectionGroupName ( connection, collection )
          // loop for each group
          for ( group <- group_list ) {
            // use different partition id, group information and associated collection
            partition_list += SequoiadbPartition ( partition_id,
                rg_map(group),
                collection)
            // increment partition id
            partition_id += 1
          } // for ( group <- group_list )
        } // for ( collection <- collections.get.toArray )
      } // if ( rg_list == None )
    } catch {
      case ex: Exception =>
        throw SequoiadbException(ex.getMessage, ex)
    } finally {
      ds.fold(ifEmpty=()) { connectionpool =>
        connection.fold(ifEmpty=()) { conn =>
          connectionpool.close(conn)
        }
        connectionpool.close
      } // ds.fold(ifEmpty=())
    } // finally
    partition_list.toArray
  }

}