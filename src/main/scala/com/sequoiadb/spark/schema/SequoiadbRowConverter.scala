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
 * Source File Name = SequoiadbRowConverter.scala
 * Description      = Record conversion between BSONObject and schema RDD
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150306 Tao Wang           Modified from original
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList
import java.sql.Timestamp
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * SequoiadbRowConverter support RDD transformations
 * from BSONObject to Row and vice versa
 */
object SequoiadbRowConverter extends JsonSupport
  with Serializable {

  /**
   *
   * @param schema RDD native schema
   * @param rdd Current native RDD
   * @return A brand new RDD of Spark SQL Row type.
   */
  def asRow(schema: StructType, rdd: RDD[BSONObject]): RDD[Row] = {
    rdd.map { record =>
      recordAsRow(dbObjectToMap(record), schema)
    }
  }

  /**
   * Given a schema, it converts a JSON object (as map) into a Row
   * @param json DBObject map
   * @param schema Schema
   * @return The converted row
   */
  def recordAsRow(
    json: Map[String, AnyRef],
    schema: StructType): Row = {
    val values: Seq[Any] = schema.fields.map {
      case StructField(name, dataType, _, _) =>
        json.get(name).flatMap(v => Option(v)).map(
          toSQL(_, dataType)).orNull
    }
    Row.fromSeq(values)
  }

  /**
   * Given a schema, it converts a Row into a DBObject
   * @param row Row to be converted
   * @param schema Schema
   * @return The converted DBObject
   */
  def rowAsDBObject(row: Row, schema: StructType): BSONObject = {
    val attMap: Map[String, Any] = schema.fields.zipWithIndex.map {
      case (att, idx) => (att.name, toDBObject(row(idx),att.dataType))
    }.toMap
    val obj: BSONObject = new BasicBSONObject ()
    obj.putAll(attMap)
    obj    
  }

  /**
   * It converts some Row attribute value into
   * a DBObject field
   * @param value Row attribute
   * @param dataType Attribute type
   * @return The converted value into a DBObject field.
   * @return The converted value into a DBObject field.
   */
  def toDBObject(value: Any, dataType: DataType): Any = {
    Option(value).map{v =>
      (dataType,v) match {
        case (ArrayType(elementType, _),array: ArrayBuffer[Any@unchecked]) => {
          val bl : BasicBSONList = new BasicBSONList
          array.zipWithIndex.foreach{
            case ( obj, i ) => bl.put(i, toDBObject(obj,elementType))
          }
          bl
        }
        case (struct: StructType,value: GenericRow) =>
          rowAsDBObject(value,struct)
        case (struct: DecimalType, value ) => value.asInstanceOf[Decimal].toDouble
        case ( struct: TimestampType, value ) => {
          val time = value.asInstanceOf[Timestamp]
          new BSONTimestamp ( (time.getTime()/1000).toInt, (time.getTime()%1000).toInt*1000)
        }
        case ( struct: ByteType, value ) => value.asInstanceOf[Byte].toInt
        case ( struct: ShortType, value ) => value.asInstanceOf[Short].toInt
        case ( struct: BinaryType, value ) => new Binary ( value.asInstanceOf[Array[Byte]] )
        case _ => v
      }
    }.orNull
  }

  /**
   * It converts some DBObject attribute value into
   * a Row field
   * @param value DBObject attribute
   * @param dataType Attribute type
   * @return The converted value into a Row field.
   */
  def toSQL(value: Any, dataType: DataType): Any = {
    Option(value).map{value =>
      dataType match {
        case ArrayType(elementType, _) =>
          value.asInstanceOf[BasicBSONList].map(toSQL(_, elementType))
        case struct: StructType =>
          recordAsRow(dbObjectToMap(value.asInstanceOf[BSONObject]), struct)
        case _ =>
          //Assure value is mapped to schema constrained type.
          enforceCorrectType(value, dataType)
      }
    }.orNull
  }

  /**
   * It creates a map with dbObject attribute values.
   * @param dBObject Object to be splitted into attribute tuples.
   * @return A map with dbObject attributes.
   */
  def dbObjectToMap(dBObject: BSONObject): Map[String, AnyRef] = {
    val scalaMap = dBObject.toMap.map {
      case ( k,v )=>(k.asInstanceOf[String] -> v.asInstanceOf[AnyRef])
    }
    scalaMap.toMap
  }

}
