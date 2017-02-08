package com.sequoiadb.spark.rdd

import org.apache.spark.SparkContext
import _root_.com.sequoiadb.spark.SequoiadbConfig
import com.sequoiadb.spark.partitioner._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.Filter
import org.apache.spark.{Partition, TaskContext}
import org.bson.BSONObject
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ArrayBuffer
//import java.io.FileOutputStream;  

/**
 * Source File Name = SequoiadbRDD.scala
 * Description      = SequoiaDB RDD
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150309 Tao Wang           Initial Draft
 */

/**
 * @param sc Spark Context
 * @param config Config parameters
 * @param partitioner Sequoiadb Partitioner object
 * @param requiredColumns Fields to project
 * @param filters Query filters
 * @param query return data type, 0 = data in bson, 1 = data in csv
 * @param query limit number, default -1 (query all data)
 */
class SequoiadbRDD(
  sc: SparkContext,
  config: SequoiadbConfig,
  partitioner: Option[SequoiadbPartitioner] = None,
  requiredColumns: Array[String] = Array(),
  filters: Array[Filter] = Array(),
  queryReturnType: Int,
  queryLimit: Long = -1)
  extends RDD[BSONObject](sc, deps = Nil) {


  private var LOG: Logger = LoggerFactory.getLogger(this.getClass.getName())
  override def getPartitions: Array[Partition] = {
    partitioner.getOrElse(new SequoiadbPartitioner(config)).computePartitions(
      filters).asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
//    split.asInstanceOf[SequoiadbPartition].hosts.map(_.getHost)
    val hostBuffer : ArrayBuffer[String] = new ArrayBuffer[String]
    val host = split.asInstanceOf[SequoiadbPartition].hosts.head
    hostBuffer += host.getHost
    hostBuffer.toList
    
  }

  override def compute(
    split: Partition,
    context: TaskContext): SequoiadbRDDIterator = {
    LOG.info ("enter SequoiadbRDD.compute")
    new SequoiadbRDDIterator(
      context,
      split.asInstanceOf[SequoiadbPartition],
      config,
      requiredColumns,
      filters,
      queryReturnType,
      queryLimit)
  }

}

object SequoiadbRDD {

  /**
   * @param sc Spark SQLContext
   * @param config Config parameters
   * @param partitioner Sequoiadb Partitioner object
   * @param requiredColumns Fields to project
   * @param filters Query filters
   * @param query return data type, 0 = data in bson, 1 = data in csv
   * @param query limit number, default -1 (query all data)
   */
  def apply (
    sc: SQLContext,
    config: SequoiadbConfig,
    partitioner: Option[SequoiadbPartitioner] = None,
    requiredColumns: Array[String] = Array(),
    filters: Array[Filter] = Array(),
    queryReturnType: Int = SequoiadbConfig.QUERYRETURNBSON,
    queryLimit: Long = -1) = {
    new SequoiadbRDD ( sc.sparkContext, config, partitioner,
      requiredColumns, filters, queryReturnType, queryLimit)
  }
}
