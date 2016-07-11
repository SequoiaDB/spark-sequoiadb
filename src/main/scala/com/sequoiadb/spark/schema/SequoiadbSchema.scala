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
/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
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
package com.sequoiadb.spark.schema

/**
 * Source File Name = SequoiadbSchema.scala
 * Description      = Sequoiadb Schema
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 */
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._
import com.sequoiadb.spark.rdd.SequoiadbRDD
import org.bson.types.BasicBSONList
import org.bson.BSONObject
import scala.collection.JavaConversions._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A custom RDD schema for SequoiaDB.
 * @param rdd RDD used to infer the schema
 * @param samplingRatio Sampling ratio used to scan the RDD and extract
 *                      used fields.
 */
case class SequoiadbSchema(
  rdd: SequoiadbRDD,
  samplingRatio: Double) extends Serializable {

  def schema(): StructType = {
    val schemaData =
      if (samplingRatio > 0.99) rdd
      else rdd.sample(withReplacement = false, samplingRatio, 1)

    val structFields = schemaData.flatMap {
      dbo => {
        val doc: Map[String, AnyRef] = SequoiadbRowConverter.dbObjectToMap(dbo)
        val fields = doc.mapValues(f => convertToStruct(f))
        fields
      }
    }.reduceByKey(compatibleType).aggregate(Seq[StructField]())(
        (fields, newField) => fields :+ StructField(newField._1, newField._2),
        (oldFields, newFields) => oldFields ++ newFields)
    StructType(structFields)
  }

  private def convertToStruct(dataType: Any): DataType = dataType match {
    case bl: BasicBSONList =>
      typeOfArray(bl)

    case bo: BSONObject => {
      val fields = bo.toMap.map {
        case (k, v) =>
          StructField(k.asInstanceOf[String], convertToStruct(v))
      }.toSeq
      StructType(fields)
    }

    case elem => 
      val typeOfObject: PartialFunction[Any, DataType] = {
        // The data type can be determined without ambiguity.
        case obj: Boolean => BooleanType
        case obj: Array[Byte] => BinaryType
        case obj: String => StringType
        case obj: UTF8String => StringType
        case obj: Byte => ByteType
        case obj: Short => ShortType
        case obj: Int => IntegerType
        case obj: Long => LongType
        case obj: Float => FloatType
        case obj: Double => DoubleType
        case obj: java.sql.Date => DateType
        case obj: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
        case obj: Decimal => DecimalType.SYSTEM_DEFAULT
        case obj: java.sql.Timestamp => TimestampType
        case null => NullType
        // For other cases, there is no obvious mapping from the type of the given object to a
        // Catalyst data type. A user should provide his/her specific rules
        // (in a user-defined PartialFunction) to infer the Catalyst data type for other types of
        // objects and then compose the user-defined PartialFunction with this one.
      }
      val elemType: PartialFunction[Any, DataType] =
        typeOfObject.orElse { case _ => StringType}
      elemType(elem)

  }

  /**
   * It looks for the most compatible type between two given DataTypes.
   * i.e.: {{{
   *   val dataType1 = IntegerType
   *   val dataType2 = DoubleType
   *   assert(compatibleType(dataType1,dataType2)==DoubleType)
   * }}}
   * @param t1
   * @param t2
   * @return
   */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonTypeOfTwo(t1, t2) match {
      case Some(commonType) => commonType

      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) =>
            val newFields = (fields1 ++ fields2)
              .groupBy(field => field.name)
              .map { case (name, fieldTypes) =>
              val dataType = fieldTypes
                .map(field => field.dataType)
                .reduce(compatibleType)
              StructField(name, dataType, nullable = true)

            }
            StructType(newFields.toSeq.sortBy(_.name))

          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(
              compatibleType(elementType1, elementType2),
              containsNull1 || containsNull2)

          case (_, _) => StringType
        }
    }
  }

  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.contains(null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements
        .map(convertToStruct)
        .reduce(compatibleType)
      ArrayType(elementType, containsNull)
    }
  }
}
