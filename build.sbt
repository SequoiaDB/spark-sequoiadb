name := "spark-sequoiadb"

version := "1.12"

organization := "com.sequoiadb"

crossScalaVersions := Seq ( "2.10.4", "2.11.0" )

libraryDependencies += "com.sequoiadb" % "sequoiadb-driver" % "1.12"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

publishMavenStyle := true

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

spName := "SequoiaDB/spark-sequoiadb"

sparkVersion := "1.6.1"

sparkComponents ++= Seq("sql")

spAppendScalaVersion := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/SequoiaDB/SequoiaDB</url>
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

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 

spIncludeMaven := true

