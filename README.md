# Spark-SequoiaDB

Spark-SequoiaDB is a library that allows users to read/write data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) from/into SequoiaDB collections.

[SequoiaDB](http://www.sequoiadb.com/ "SequoiaDB website") is a document-oriented NoSQL database and provides a JSON storage model. [Spark](http://spark.apache.org/ "Spark website") is a fast and general-purpose cluster computing system.

Spark-SequoiaDB library is used to integrate SequoiaDB and Spark, in order to give users a system that combines the advantages of schema-less storage model with dynamic indexing and Spark cluster.

## Requirements

This library requires Spark 1.3+, Scala 2.10.4+ and sequoiadb-driver-1.12

## Using the library

You can link against this library by putting the following lines in your program:
```
<groupId>com.sequoiadb</groupId>
<artifactId>spark-sequoiadb</artifactId>
<version>LATEST</version>
```
You can also download the project separately by doing:
```
git clone https://github.com/SequoiaDB/spark-sequoiadb.git
mvn clean install
```
You can load the library into spark-shell by using --jars command line option.  
$ bin/spark-shell --jars /Users/sequoiadb/spark-sequoiadb/lib/sequoiadb-driver-1.12.0.jar,/Users/sequoiadb/spark-sequoiadb/target/spark-sequoiadb-1.12.0-SNAPSHOT.jar  
…  
15/03/09 14:35:45 INFO HttpServer: Starting HTTP Server  
15/03/09 14:35:45 INFO Utils: Successfully started service 'HTTP class server' on port 59998.  
Welcome to  
```
      ____              __  
     / __/__  ___ _____/ /__  
    _\ \/ _ \/ _ `/ __/  '_/  
   /___/ .__/\_,_/_/ /_/\_\   version 1.3.0  
      /_/  
```
  
Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_75)  
Type in expressions to have them evaluated.  
Type :help for more information.  
…  
15/03/09 14:35:51 INFO SparkILoop: Created spark context..  
Spark context available as sc.  
15/03/09 14:35:52 INFO SparkILoop: Created sql context..  
SQL context available as sqlContext.  
  
scala> sqlContext.sql("CREATE temporary table foo ( hello string, rangekey int, key1 int ) using com.sequoiadb.spark OPTIONS ( host '10.0.2.20:11810', collectionspace ‘macs’, collection ‘mycl’)”)  
res0: org.apache.spark.sql.DataFrame = []  
  
scala> sqlContext.sql("select * from foo").foreach(println)  
/Users/sequoiadb/spark/spark/sdbdriver.properties  
15/03/09 14:37:59 INFO SparkContext: Starting job: foreach at DataFrame.scala:797  
…  
[world,350,1]  
[world,350,null]  
…  
[12345,150,500]  
[nihao,100,10]  
…  
[good,10,5]  
[good,10,5]  
…  
15/03/09 14:38:00 INFO DAGScheduler: Job 0 finished: foreach at DataFrame.scala:797, took 0.616590 s  
  
scala> sqlContext.sql("CREATE TEMPORARY TABLE jsontable ( hello string, rangekey int, key1 int ) using org.apache.spark.sql.json.DefaultSource options ( path '/Users/sequoiadb/temp/test.json' )")  
res2: org.apache.spark.sql.DataFrame = []  
  
scala> sqlContext.sql("select * from jsontable").foreach(println)  
…  
[this is a new hello message,310,-10]  
[100000,30,0]  
…  
  
scala> sqlContext.sql("insert into table foo select * from jsontable")  
…  
res4: org.apache.spark.sql.DataFrame = [hello: string, rangekey: int, key1: int]  
  
sqlContext.sql("select * from foo").foreach(println)  
…  
[world,350,null]  
…  
[world,350,1]  
[this is a new hello message,310,-10]  
…  
[12345,150,500]  
[nihao,100,10]  
[good,10,5]  
[good,10,5]  
[100000,30,0]  
  
## License

Licensed to SequoiaDB (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership. The SequoiaDB (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
