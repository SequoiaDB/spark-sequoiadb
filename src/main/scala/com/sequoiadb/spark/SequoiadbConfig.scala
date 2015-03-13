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
  def get[T:ClassTag](property: Property): Option[T] =
    properties.get(property).map(_.asInstanceOf[T])

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
  val Preference      = "preference"
  val Username        = "username"
  val Password        = "password"
  val BulkSize        = 500

  val all = List(
    Host,
    CollectionSpace,
    Collection,
    SamplingRatio,
    Preference,
    Username,
    Password)

  //  Default values

  val DefaultSamplingRatio = 1.0
  val DefaultPreference = ""
  val DefaultPort = "11810"
  val DefaultUsername = ""
  val DefaultPassword = ""

  val Defaults = Map(
    SamplingRatio -> DefaultSamplingRatio,
    Preference -> DefaultPreference)
}