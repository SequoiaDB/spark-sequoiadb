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
 * Source File Name = JsonSupport.scala
 * Description      = Helper functions for JSON
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150306 Tao Wang           Modified toXXX functions for more general support
 */
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal
import java.sql.Timestamp
import java.sql.Date
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.ObjectId
import java.text.SimpleDateFormat;
import com.sequoiadb.spark.util.ByteUtil

/**
 * Json - Scala object transformation support.
 * Used to convert from DBObjects to Spark SQL Row field types.
 * Disclaimer: As explained in NOTICE.md, some of this product includes
 * software developed by The Apache Software Foundation (http://www.apache.org/).
 */
trait JsonSupport {

  /**
   * Tries to convert some scala value to another compatible given type
   * @param value Value to be converted
   * @param desiredType Destiny type
   * @return Converted value
   */
  protected def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      try {
        desiredType match {
          case StringType => toString(value)
          case _ if value == null || value == "" => null // guard the non string type
          case IntegerType => toInt(value)
          case LongType => toLong(value)
          case DoubleType => toDouble(value)
          case DecimalType() => toDecimal(value)
          case BooleanType => toBoolean(value)
          case NullType => null
          case TimestampType => toTimestamp(value)
          case FloatType => toFloat(value)
          case ByteType => toByte(value)
          case ShortType => toShort(value)
          case DateType => toDate(value)
          case BinaryType => toBinary(value)
          case ArrayType(elementType, _) =>
            value.asInstanceOf[Seq[Any]].map(enforceCorrectType(_, elementType))
          case MapType(StringType, valueType, _) => {
            val map = value.asInstanceOf[Map[String, Any]]
            map.mapValues(enforceCorrectType(_, valueType)).map(identity)
          }
          case struct: StructType => SequoiadbRowConverter.recordAsRow(
              value.asInstanceOf[Map[String, AnyRef]], struct)
          case _ =>
            sys.error(s"Unsupported datatype conversion [${value.getClass}},$desiredType]")
            value
        }
      }
      catch {
        case ex: Exception => null
      }
    }
  }
  
  private def toShort(value: Any) : Short = {
    value match {
      case value: java.lang.Float => value.toShort
      case value: java.lang.Double => value.toShort
      case value: java.lang.Integer => value.toShort
      case value: java.lang.Long => value.toShort
      case value: java.lang.Short => value.toShort
      case value: java.lang.Byte => value.toShort
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.shortValue
      case value: java.math.BigDecimal => value.shortValue
      case value: BSONTimestamp=>value.getTime().toShort
      case value: java.util.Date=>value.getTime().toShort
      case value: String => value.toShort
      case _ => 0
    }
  }
  
  private def toBoolean(value: Any) : Boolean = {
    value match {
      case value: java.lang.Float => if ( value == 0 ) false else true
      case value: java.lang.Double => if ( value == 0 ) false else true
      case value: java.lang.Integer => if ( value == 0 ) false else true
      case value: java.lang.Long => if ( value == 0 ) false else true
      case value: java.lang.Short => if ( value == 0 ) false else true
      case value: java.lang.Byte => if ( value == 0 ) false else true
      case value: java.lang.Boolean => value
      case value: java.math.BigInteger => if ( value == 0 ) false else true
      case value: java.math.BigDecimal => if ( value == 0 ) false else true
      case _ => false
    }
  }
  
  private def toByte(value: Any) : Byte = {
    value match {
      case value: java.lang.Float => value.toByte
      case value: java.lang.Double => value.toByte
      case value: java.lang.Integer => value.toByte
      case value: java.lang.Long => value.toByte
      case value: java.lang.Short => value.toByte
      case value: java.lang.Byte => value.toByte
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.byteValue
      case value: java.math.BigDecimal => value.byteValue
      case value: BSONTimestamp=>value.getTime().toByte
      case value: java.util.Date=>value.getTime().toByte
      case value: String => value.toByte
      case _ => 0
    }
  }
  
  private def toFloat(value: Any) : Float = {
    value match {
      case value: java.lang.Float => value.toFloat
      case value: java.lang.Double => value.toFloat
      case value: java.lang.Integer => value.toFloat
      case value: java.lang.Long => value.toFloat
      case value: java.lang.Short => value.toFloat
      case value: java.lang.Byte => value.toFloat
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.floatValue
      case value: java.math.BigDecimal => value.floatValue
      case value: BSONTimestamp=>value.getTime().toFloat
      case value: java.util.Date=>value.getTime().toFloat
      case value: String => value.toFloat
      case _ => 0
    }
  }
  
  private def toTimestamp(value: Any) : Timestamp = {
    value match {
      case value: java.lang.Integer=>new Timestamp(value.toLong)
      case value: java.lang.Long=>new Timestamp(value)
      case value: BSONTimestamp=>new Timestamp((value.getTime().toLong*1000))
      case value: java.util.Date=>new Timestamp(value.getTime())
      case value: String=> new Timestamp((new SimpleDateFormat(
          "yyyy-MM-dd.HH:mm:ss")).parse(value).getTime())
      case _ => new Timestamp(0)
    }
  }
  
  private def toDate(value: Any) : Date = {
    new java.sql.Date(toTimestamp(value).getTime)
  }

  private def toInt(value: Any): Int = {
    value match {
      case value: java.lang.Float => value.toInt
      case value: java.lang.Double => value.toInt
      case value: java.lang.Integer => value.toInt
      case value: java.lang.Long => value.toInt
      case value: java.lang.Short => value.toInt
      case value: java.lang.Byte => value.toInt
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.intValue
      case value: java.math.BigDecimal => value.intValue
      case value: BSONTimestamp=>value.getTime().toInt
      case value: java.util.Date=>value.getTime().toInt
      case value: String => value.toInt
      case _ => 0
    }
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Float => value.toLong
      case value: java.lang.Double => value.toLong
      case value: java.lang.Integer => value.toLong
      case value: java.lang.Long => value.toLong
      case value: java.lang.Short => value.toLong
      case value: java.lang.Byte => value.toLong
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.longValue
      case value: java.math.BigDecimal => value.longValue
      case value: BSONTimestamp=>value.getTime().toLong
      case value: java.util.Date=>value.getTime().toLong
      case value: String => value.toLong
      case _ => 0
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Float => value.toDouble
      case value: java.lang.Double => value.toDouble
      case value: java.lang.Integer => value.toDouble
      case value: java.lang.Long => value.toDouble
      case value: java.lang.Short => value.toDouble
      case value: java.lang.Byte => value.toDouble
      case value: java.lang.Boolean => if ( value == false ) 0 else 1
      case value: java.math.BigInteger => value.doubleValue
      case value: java.math.BigDecimal => value.doubleValue
      case value: BSONTimestamp=>value.getTime().toDouble
      case value: java.util.Date=>value.getTime().toDouble
      case value: String => value.toDouble
      case _ => 0
    }
  }

  private def toDecimal(value: Any): Decimal = {
    value match {
      case value: java.lang.Integer => Decimal(value)
      case value: java.lang.Long => Decimal(value)
      case value: java.lang.Float => Decimal(value.toDouble)
      case value: java.lang.Double => Decimal(value)
      case value: java.lang.Short => Decimal(value.toInt)
      case value: java.lang.Byte => Decimal(value.toInt)
      case value: java.lang.Boolean => if ( value == false ) Decimal(0) else Decimal(1)
      case value: java.math.BigInteger => Decimal(new java.math.BigDecimal(value))
      case value: java.math.BigDecimal => Decimal(value)
      case value: BSONTimestamp=>Decimal(value.getTime().toLong)
      case value: java.util.Date=>Decimal(value.getTime().toLong)
      case value: String => Decimal(value)
      case _ => Decimal(0)
    }
  }
  
  private def toBinary ( value: Any ) : Array[Byte] = {
    value match {
      case value: java.lang.Float => ByteUtil.getBytes(value)
      case value: java.lang.Double => ByteUtil.getBytes(value)
      case value: java.lang.Integer => ByteUtil.getBytes(value)
      case value: java.lang.Long => ByteUtil.getBytes(value)
      case value: java.lang.Short => ByteUtil.getBytes(value)
      case value: java.lang.Byte => Array[Byte](value)
      case value: java.lang.Boolean => if ( value == false ) Array[Byte](0) else Array[Byte](1)
      case value: java.math.BigInteger => ByteUtil.getBytes(value.longValue)
      case value: java.math.BigDecimal => ByteUtil.getBytes(value.doubleValue)
      case value: BSONTimestamp=>ByteUtil.getBytes(value.getTime().toLong)
      case value: java.util.Date=>ByteUtil.getBytes(value.getTime().toLong)
      case value: String => ByteUtil.getBytes(value)
      case value: Binary => value.getData
      case value: ObjectId => value.toByteArray()
      case _ => Array[Byte]()
    }
  }

  private def toJsonArrayString(seq: Seq[Any]): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any]): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        val stringValue = if (value.isInstanceOf[String]) s"""\"$value\"""" else toString(value)
        builder.append(s"""\"$key\":$stringValue""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]])
      case value: Seq[_] => toJsonArrayString(value)
      case v => Option(v).map(_.toString).orNull
    }
  }

}

