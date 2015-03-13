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
package com.sequoiadb.spark.partitioner

/**
 * Source File Name = SequoiadbHost.scala
 * Description      = Host information SequoiaDB
 * When/how to use  = SequoiadbHost class is used to represent hostname+port for sdb connection
 * Restrictions     = N/A
 * Change Activity:
 * Date     Who                Description
 * ======== ================== ================================================
 * 20150305 Tao Wang           Initial Draft
 */
import com.sequoiadb.spark.SequoiadbConfig
import scala.collection.mutable.ArrayBuffer

/**
 * SequoiadbHostBuilder is used to build host list
 * @param hostlist List of hostname and port number separated by ","
 */
case class SequoiadbHostBuilder (
    hostlist: String ) extends Serializable {
  // split hostlist string by ',' and construct 
  val host_list: ArrayBuffer[SequoiadbHost] = hostlist.split(',').map(SequoiadbHost(_)).to[ArrayBuffer]
  /**
   * add a new host into builder
   * @param host SequoiadbHost object
   */
  def add ( host: SequoiadbHost ): Unit = {
    if ( !host_list.exists ( _.equals(host) ) ) host_list += host
  }
  /**
   * add a new host into builder
   * @param host hostname string
   */
  def add ( host: String ): Unit = {
    add ( SequoiadbHost ( host ))
  }
  override def toString = build.mkString (",")
  /**
   * create an Array[SequoiadbHost]
   */
  def build = host_list.toArray
}

object SequoiadbHostBuilder {
  /**
   * Construct host builder by providing a list of SequoiadbHost
   */
  def apply ( hostlist: Array[SequoiadbHost] ) = {
    new SequoiadbHostBuilder ( hostlist.mkString ( "," ))
  }
  /**
   * Construct host builder by providing a single SequoiadbHost
   */
  def apply ( host: SequoiadbHost ) = {
    new SequoiadbHostBuilder ( host.toString() )
  }
}

/**
 * @param hostname Hostname
 * @param service Service name, could be a port number
 */
case class SequoiadbHost(
  hostname: String,
  service: Option[String]) extends Serializable {
  
  val getHost : String = hostname.trim()
  val getService : String = service.getOrElse(SequoiadbConfig.DefaultPort).trim()
  override def toString () = getHost + ":" + getService
  override def equals ( o: Any ) = o match {
    case that: SequoiadbHost => {
      that.getHost.compareTo(this.getHost) == 0 &&
      that.getService.compareTo(this.getService) == 0
    }
    case _ => false
  }
}

object SequoiadbHost {
  /**
   * Convert a connect string into SequoiadbHost object
   * Connect string format: <hostname[:servicename]>
   * ex:
   * hostname
   * hostname:port
   * @param connect Connect string
   */
  def apply ( connect: String ) = {
    val host_port = connect.split(":")
    new SequoiadbHost ( host_port(0),
      if ( host_port.length == 1 ) None 
      else Some(host_port(1)))
  }
  
}