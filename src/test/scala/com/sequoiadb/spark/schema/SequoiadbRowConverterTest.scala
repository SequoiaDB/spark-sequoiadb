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
package com.sequoiadb.spark.schema

import org.bson.BSONObject
import org.bson.util.JSON
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.BasicBSONList
import java.util.Date
import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
/**
 * @author sequoiadb
 */
class SequoiadbRowConverterTest extends FlatSpec
with Matchers{
  val primitiveSchema = StructType(
      StructField("decimal", DecimalType.Unlimited, true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("float", FloatType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", IntegerType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", NullType, true) ::
      StructField("byte", ByteType, true) ::
      StructField("short", ShortType, true) ::
      StructField("timestamp", TimestampType, true) ::
      StructField("date", DateType, true) ::
      StructField("binary", BinaryType, true) ::
      StructField("string", StringType, true) :: Nil)
      
  val embedSchema = StructType(
      StructField("firstDecimal", DecimalType.Unlimited, true) ::
      StructField("firstString", StringType, true) ::
      StructField("firstObject", StructType(
          StructField("secondInt",IntegerType,true)::
          StructField("secondBoolean",BooleanType,true)::Nil), true) ::
      StructField("firstArray", ArrayType(
          StructType(
              StructField("thirdDate",DateType,true) ::
              StructField("thirdString",StringType,true)::Nil
            )
          ), true) :: Nil)

  val allStringSchema1 = StructType(
      StructField("decimal", StringType, true) ::
      StructField("boolean", StringType, true) ::
      StructField("float", StringType, true) ::
      StructField("double", StringType, true) ::
      StructField("integer", StringType, true) ::
      StructField("long", StringType, true) ::
      StructField("null", StringType, true) ::
      StructField("byte", StringType, true) ::
      StructField("short", StringType, true) ::
      StructField("timestamp", StringType, true) ::
      StructField("date", StringType, true) ::
      StructField("binary", StringType, true) ::
      StructField("string", StringType, true) :: Nil)
      
  val allStringSchema2 = StructType(
      StructField("firstDecimal", StringType, true) ::
      StructField("firstString", StringType, true) ::
      StructField("firstObject", StringType, true ) ::
      StructField("firstArray", ArrayType(
          StructType(
              StructField("thirdDate",StringType,true) ::
              StructField("thirdString",StringType,true)::Nil
            )
          ), true) :: Nil)

  "Normal schema" should "be able to handle normal data" in {
    val primitivedata : BSONObject = JSON.parse( TestJsonData.primitiveObject ).asInstanceOf[BSONObject]
    val result : BSONObject = SequoiadbRowConverter.rowAsDBObject(
        SequoiadbRowConverter.recordAsRow(
          SequoiadbRowConverter.dbObjectToMap(primitivedata),
          primitiveSchema),
        primitiveSchema)
    result.get("decimal") should equal (primitivedata.get("decimal"))
    result.get("boolean") should equal (primitivedata.get("boolean"))
    // float and double type conversion cannot make direct comparison
    result.get("float").toString should equal (primitivedata.get("float").toString)
    result.get("double") should equal (primitivedata.get("double"))
    result.get("integer") should equal (primitivedata.get("integer"))
    result.get("long") should equal (primitivedata.get("long"))
    result.get("null") should equal (primitivedata.get("null"))
    result.get("byte") should equal (primitivedata.get("byte"))
    result.get("short") should equal (primitivedata.get("short"))
    // compare BSONTimestamp and java.sql.Timestamp
    result.get("timestamp") should equal (primitivedata.get("timestamp"))
    result.get("date") should equal (primitivedata.get("date"))
    result.get("binary").asInstanceOf[Binary].getData should equal (
        primitivedata.get("binary").asInstanceOf[Binary].getData)
    result.get("string") should equal ( primitivedata.get("string"))
  }
  
  "Embed schema" should "be able to handle embed data" in {
    val embeddata : BSONObject = JSON.parse( TestJsonData.embedObject ).asInstanceOf[BSONObject]
    val result : BSONObject = SequoiadbRowConverter.rowAsDBObject(
        SequoiadbRowConverter.recordAsRow(
          SequoiadbRowConverter.dbObjectToMap(embeddata),
          embedSchema),
        embedSchema)
    result.get("firstDecimal") should equal (embeddata.get("firstDecimal"))
    result.get("firstString") should equal (embeddata.get("firstString"))
    result.get("firstObject") should equal ( embeddata.get("firstObject"))
    result.get("firstArray") should equal ( embeddata.get("firstArray"))
  }
  
  "String schema" should "be able to handle primitive data types" in {
    val primitivedata : BSONObject = JSON.parse( TestJsonData.primitiveObject ).asInstanceOf[BSONObject]
    val result : BSONObject = SequoiadbRowConverter.rowAsDBObject(
        SequoiadbRowConverter.recordAsRow(
          SequoiadbRowConverter.dbObjectToMap(primitivedata),
          allStringSchema1),
        allStringSchema1)
    result.get("decimal") should equal (primitivedata.get("decimal").toString)
    result.get("boolean") should equal (primitivedata.get("boolean").toString)
    // float and double type conversion cannot make direct comparison
    result.get("float").toString should equal (primitivedata.get("float").toString)
    result.get("double") should equal (primitivedata.get("double").toString)
    result.get("integer") should equal (primitivedata.get("integer").toString)
    result.get("long") should equal (primitivedata.get("long").toString)
    // both side should be null
    result.get("null") should equal (result.get("null"))
    result.get("byte") should equal (primitivedata.get("byte").toString)
    result.get("short") should equal (primitivedata.get("short").toString)
    // compare BSONTimestamp and java.sql.Timestamp
    result.get("timestamp") should equal (primitivedata.get("timestamp").toString)
    result.get("date") should equal (primitivedata.get("date").toString)
    result.get("binary") should equal (primitivedata.get("binary").toString)
    result.get("string") should equal ( primitivedata.get("string"))
  }
  
  it should "be able to handle embed data types" in {
    val embeddata : BSONObject = JSON.parse( TestJsonData.embedObject ).asInstanceOf[BSONObject]
    val result : BSONObject = SequoiadbRowConverter.rowAsDBObject(
        SequoiadbRowConverter.recordAsRow(
          SequoiadbRowConverter.dbObjectToMap(embeddata),
          allStringSchema2),
        allStringSchema2)
    result.get("firstDecimal") should equal (embeddata.get("firstDecimal").toString)
    result.get("firstString") should equal (embeddata.get("firstString").toString)
    result.get("firstObject") should equal ( embeddata.get("firstObject").toString)
    val resultList = result.get("firstArray").asInstanceOf[BasicBSONList].toArray.toList
    val embedList = embeddata.get("firstArray").asInstanceOf[BasicBSONList].toArray.toList
    resultList.length should equal ( embedList.length )
    resultList.zip(embedList).map { case (res,ori)=>
      res.asInstanceOf[BSONObject].get("thirdDate") should equal ( 
          ori.asInstanceOf[BSONObject].get("thirdDate").toString)
      res.asInstanceOf[BSONObject].get("thirdString") should equal ( 
          ori.asInstanceOf[BSONObject].get("thirdString").toString)
    }
  }
}