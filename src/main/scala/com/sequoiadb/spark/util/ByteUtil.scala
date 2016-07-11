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

/**
 * Source File Name = ByteUtil.scala
 * Description      = Byte utilities
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150306 Tao Wang           Initial Draft
 */
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.Charset

object ByteUtil {
  def getBytes ( data: Short) : Array[Byte] = {
    ArrayBuffer[Byte](
        (data&0xff).toByte, 
        ((data&0xff00)>>8).toByte).toArray
  }
  def getBytes ( data: Char) : Array[Byte] = {
    ArrayBuffer[Byte](
        (data&0xff).toByte).toArray
  }
  def getBytes ( data: Int) : Array[Byte] = {
    ArrayBuffer[Byte](
        ((data&0xff).toByte),
        ((data&0xff00)>>8).toByte,
        ((data&0xff0000)>>16).toByte,
        ((data&0xff000000)>>24).toByte).toArray
  }
  def getBytes ( data: Long) : Array[Byte] = {
    ArrayBuffer[Byte](
        ((data&0xff).toByte),
        ((data>>8)&0xff).toByte,
        ((data>>16)&0xff).toByte,
        ((data>>24)&0xff).toByte,
        ((data>>32)&0xff).toByte,
        ((data>>40)&0xff).toByte,
        ((data>>48)&0xff).toByte,
        ((data>>56)&0xff).toByte).toArray
  }
  def getBytes ( data: Float) : Array[Byte] = {
    getBytes ( java.lang.Float.floatToIntBits(data))
  }
  def getBytes ( data: Double) : Array[Byte] = {
    getBytes ( java.lang.Double.doubleToLongBits(data))
  }
  def getBytes ( data: String, charsetName: String = "UTF-8" ): Array[Byte] = {
    data.getBytes ( Charset.forName(charsetName) )
  }
}