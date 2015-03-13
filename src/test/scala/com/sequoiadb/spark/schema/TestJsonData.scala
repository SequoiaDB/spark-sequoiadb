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

import org.apache.spark.sql.test.TestSQLContext

object TestJsonData {

  val primitiveObject ="{" +
    "decimal: 1024.1024,"+
    "boolean: true,"+
    "float: 123.123,"+
    "byte: 127,"+
    "short: 32000,"+
    "timestamp: {$timestamp:\"2015-01-01-05.04.03.000000\" },"+
    "date: {$date:\"2015-01-01\" },"+
    "binary: {$binary:\"dGhpcyBpcyBiaW5hcnkgc3RyaW5n\",$type:\"1\" },"+
    "string:\"this is a simple string.\","+
    "integer:10,"+
    "long:21474836470,"+
    "double:1.7976931348623157E308,"+
    "boolean:true,"+
    "null:null"+
    "}"
  val embedObject = "{" +
    "firstDecimal: 1024.1024,"+
    "firstString: \"this is a simple string\","+
    "firstObject: {"+
      "secondInt: 1234567,"+
      "secondBoolean: false"+
    "},"+
    "firstArray: ["+
      "{"+
        "thirdDate:{$date:\"2014-05-04\"},"+
        "thirdString:\"this is third string1\""+
      "},"+
      "{"+
        "thirdDate:{$date:\"2015-01-01\"},"+
        "thirdString:\"this is third string2\""+
      "}"+
    "]"+
  "}"
}
