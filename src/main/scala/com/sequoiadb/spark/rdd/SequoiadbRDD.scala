package com.sequoiadb.spark.rdd

import com.sequoiadb.spark.SequoiadbConfig
import com.sequoiadb.spark.partitioner._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.Filter
import org.apache.spark.{Partition, TaskContext}
import org.bson.BSONObject
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
 * @param sc Spark SQLContext
 * @param config Config parameters
 * @param requiredColumns Fields to project
 * @param filters Query filters
 */
class SequoiadbRDD(
  sc: SQLContext,
  config: SequoiadbConfig,
  partitioner: SequoiadbPartitioner,
  requiredColumns: Array[String] = Array(),
  filters: Array[Filter] = Array())
  extends RDD[BSONObject](sc.sparkContext, deps = Nil) {
  
  override def getPartitions: Array[Partition] =
    partitioner.computePartitions(filters).asInstanceOf[Array[Partition]]

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[SequoiadbPartition].hosts.map(_.getHost)

  override def compute(
    split: Partition,
    context: TaskContext): SequoiadbRDDIterator =
    new SequoiadbRDDIterator(
      context,
      split.asInstanceOf[SequoiadbPartition],
      config,
      requiredColumns,
      filters)

}