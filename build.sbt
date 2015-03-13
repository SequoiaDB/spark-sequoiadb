name := "spark-sequoiadb"

version := "0.0.1"

organization := "com.sequoiadb"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0-rc2" % "provided"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" % "provided"

//libraryDependencies += "com.sequoiadb" % "sequoiadb-driver" % "1.10"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/SequoiaDB/SequoiaDB</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:SequoiaDB/SequoiaDB.git</url>
    <connection>scm:git:git@github.com:SequoiaDB/SequoiaDB.git</connection>
  </scm>
  <developers>
    <developer>
      <id>taoewang</id>
      <name>Tao Wang</name>
      <url>http://www.sequoiadb.com</url>
    </developer>
  </developers>)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
