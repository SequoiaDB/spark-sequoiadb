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

import org.scalatest.{FlatSpec, Matchers}
import collection.mutable.Stack

/**
 * Source File Name = ByteUtilTest.scala
 * Description      = Testcase for ByteUtil class
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150309 Tao Wang           Initial Draft
 */
class ByteUtilTest extends FlatSpec
with Matchers{
  val testCharPair: Tuple2[Char,Array[Byte]] = ('a', Array[Byte](
      0x61.asInstanceOf[Byte]) )
  val testShortPair: Tuple2[Short,Array[Byte]] = (270, Array[Byte](
      0x0E.asInstanceOf[Byte],
      0x01.asInstanceOf[Byte]) )
  val testIntPair: Tuple2[Integer,Array[Byte]] = (12345678, Array[Byte](
      0x4E.asInstanceOf[Byte],
      0x61.asInstanceOf[Byte],
      0xBC.asInstanceOf[Byte],
      0x00.asInstanceOf[Byte]) )
  val testLongPair: Tuple2[Long,Array[Byte]] = (123456789012345678L, Array[Byte](
      0x4E.asInstanceOf[Byte],
      0xF3.asInstanceOf[Byte],
      0x30.asInstanceOf[Byte],
      0xA6.asInstanceOf[Byte],
      0x4B.asInstanceOf[Byte],
      0x9B.asInstanceOf[Byte],
      0xB6.asInstanceOf[Byte],
      0x01.asInstanceOf[Byte]) )
  val testFloatPair: Tuple2[Float,Array[Byte]] = (1234567.12345f, Array[Byte](
      0x39.asInstanceOf[Byte],
      0xB4.asInstanceOf[Byte],
      0x96.asInstanceOf[Byte],
      0x49.asInstanceOf[Byte]) )
  val testDoublePair: Tuple2[Double,Array[Byte]] = (123456789.123456789, Array[Byte](
      0x75.asInstanceOf[Byte],
      0x6B.asInstanceOf[Byte],
      0x7E.asInstanceOf[Byte],
      0x54.asInstanceOf[Byte],
      0x34.asInstanceOf[Byte],
      0x6F.asInstanceOf[Byte],
      0x9D.asInstanceOf[Byte],
      0x41.asInstanceOf[Byte]) )
   val testEnglishStringPair: Tuple2[String,Array[Byte]] = ("hello world", Array[Byte](
      0x68.asInstanceOf[Byte],
      0x65.asInstanceOf[Byte],
      0x6C.asInstanceOf[Byte],
      0x6C.asInstanceOf[Byte],
      0x6F.asInstanceOf[Byte],
      0x20.asInstanceOf[Byte],
      0x77.asInstanceOf[Byte],
      0x6F.asInstanceOf[Byte],
      0x72.asInstanceOf[Byte],
      0x6C.asInstanceOf[Byte],
      0x64.asInstanceOf[Byte]) )
   val testChineseStringPair: Tuple2[String,Array[Byte]] = ("世界你好", Array[Byte](
      0xE4.asInstanceOf[Byte],
      0xB8.asInstanceOf[Byte],
      0x96.asInstanceOf[Byte],
      0xE7.asInstanceOf[Byte],
      0x95.asInstanceOf[Byte],
      0x8C.asInstanceOf[Byte],
      0xE4.asInstanceOf[Byte],
      0xBD.asInstanceOf[Byte],
      0xA0.asInstanceOf[Byte],
      0xE5.asInstanceOf[Byte],
      0xA5.asInstanceOf[Byte],
      0xBD.asInstanceOf[Byte]) )
  "ByteUtil" should "be able to handle Char" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testCharPair._1)
    arr.length should equal ( testCharPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testCharPair._2(i))}
  }
  it should "be able to handle Short" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testShortPair._1)
    arr.length should equal ( testShortPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testShortPair._2(i))}
  }
  it should "be able to handle Integer" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testIntPair._1)
    arr.length should equal ( testIntPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testIntPair._2(i))}
  }
  it should "be able to handle Long" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testLongPair._1)
    arr.length should equal ( testLongPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testLongPair._2(i))}
  }
  it should "be able to handle Float" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testFloatPair._1)
    arr.length should equal ( testFloatPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testFloatPair._2(i))}
  }
  it should "be able to handle Double" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testDoublePair._1)
    arr.length should equal ( testDoublePair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testDoublePair._2(i))}
  }
  it should "be able to handle English String" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testEnglishStringPair._1)
    arr.length should equal ( testEnglishStringPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testEnglishStringPair._2(i))}
  }
  it should "be able to handle Chinese String" in {
    val arr: Array[Byte] = ByteUtil.getBytes(testChineseStringPair._1)
    arr.length should equal ( testChineseStringPair._2.length )
    arr.zipWithIndex.map { case ( s, i ) => s should equal ( testChineseStringPair._2(i))}
  }
}
