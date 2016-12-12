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
package com.sequoiadb.spark

/**
 * Source File Name = SequoiadbConfig.scala
 * Description      = SequoiaDB Configuration
 * When/how to use  = Used when initializing SequoiadbRDD
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */

import _root_.com.sequoiadb.spark.SequoiadbConfig.Property
import scala.reflect.ClassTag

case class SequoiadbConfigBuilder(
  val properties: Map[Property,Any] = Map()) extends Serializable { build =>

  val requiredProperties: List[Property] = SequoiadbConfig.all

  /**
   * Instantiate a brand new Builder from given properties map
   *
   * @param props Map of any-type properties.
   * @return The new builder
   */
  def apply(props: Map[Property, Any]) =
    SequoiadbConfigBuilder(props)

  /**
   * Set (override if exists) a single property value given a new one.
   *
   * @param property Property to be set
   * @param value New value for given property
   * @tparam T Property type
   * @return A new builder that includes new value of the specified property
   */
  def set[T](property: Property,value: T): SequoiadbConfigBuilder =
    apply(properties + (property -> value))
    
  /**
   * Build the config object from current builder properties.
   *
   * @return The SequoiaDB configuration object.
   */
  def build(): SequoiadbConfig = new SequoiadbConfig(properties) {
    require(
      requiredProperties.forall(properties.isDefinedAt),
      s"Not all properties are defined! : ${
        requiredProperties.diff(
          properties.keys.toList.intersect(requiredProperties))
      }")

  }
}

class SequoiadbConfig (
  val properties: Map[Property,Any] = Map()) extends Serializable {

  /**
   * Gets specified property from current configuration object
   * @param property Desired property
   * @tparam T Property expected value type.
   * @return An optional value of expected type
   */
  def get[T:ClassTag](property: Property): Option[T] = {
    val t = properties.get(property).map(_.asInstanceOf[T])
    if (t == None || t == Option ("")) {
      return SequoiadbConfig.Defaults.get (property).map(_.asInstanceOf[T])
    }
    if (property.equals(SequoiadbConfig.Preference)) {
      val _preferenceValue = t.get.asInstanceOf[String]
      // preferenceValue should equal "m"/"M"/"s"/"S"/"a"/"A"/"1-7"
      val preferenceValue = _preferenceValue match {
        case "m" => "m"
        case "M" => "M"
        case "s" => "s"
        case "S" => "S"
        case "a" => "a"
        case "A" => "A"
        case "1" => "1"
        case "2" => "2"
        case "3" => "3"
        case "4" => "4"
        case "5" => "5"
        case "6" => "6"
        case "7" => "7"
        case _   => SequoiadbConfig.DefaultPreference
      }
      return Option (
            ("{PreferedInstance:\"" + preferenceValue + "\"}").asInstanceOf[T]
          )
    }
    if (property.equals(SequoiadbConfig.BulkSize)) {
      val _bulkSizeValue = t.get.asInstanceOf[String].toInt
      if (_bulkSizeValue <= 0) {
        return Option (
          (SequoiadbConfig.DefaultBulkSize).asInstanceOf[T]    
        )
      }
    }
    t
  }

  /**
   * Gets specified property from current configuration object.
   * It will fail if property is not previously set.
   * @param property Desired property
   * @tparam T Property expected value type
   * @return Expected type value
   */
  def apply[T:ClassTag](property: Property): T =
    get[T](property).get
}


object SequoiadbConfig {
  type Property = String
  def notFound[T](key: String): T =
    throw new IllegalStateException(s"Parameter $key not specified")
  //  Parameter names

  val Host            = "host"
  val CollectionSpace = "collectionspace"
  val Collection      = "collection"
  val SamplingRatio   = "samplingRatio"
  val Preference      = "preference"  // "m"/"M"/"s"/"S"/"a"/"A"/"1-7"
  val Username        = "username"
  val Password        = "password"
  val ScanType        = "scantype"    // auto/ixscan/tbscan
  val BulkSize        = "bulksize"    // default 512
  val scanTypeExplain = 0
  val scanTypeGetQueryMeta = 1

  val all = List(
    Host,
    CollectionSpace,
    Collection,
    SamplingRatio,
    Preference,
    Username,
    Password,
    ScanType,
    BulkSize
    )

  //  Default values

  val DefaultSamplingRatio = 1.0
  val DefaultPreference = "S"
  val DefaultPort = "11810"
  val DefaultHost = "localhost"
  val DefaultUsername = ""
  val DefaultPassword = ""
  val DefaultScanType = "auto"
  val DefaultBulkSize = "512"

  val Defaults = Map(
    SamplingRatio -> DefaultSamplingRatio,
    Preference -> ("{PreferedInstance:\"" + DefaultPreference + "\"}"),
    ScanType -> DefaultScanType,
    Host -> List(DefaultHost + ":" + DefaultPort),
    Username -> (DefaultUsername),
    Password -> (DefaultPassword),
    BulkSize -> (DefaultBulkSize)
    )
}
