name := "bts-rtda-lab-9"
version := "1"
scalaVersion := "2.11.0"

val sparkVersion = "2.4.3" //Version of scala to be used on app

val sparkTestingBase = "2.4.0_0.11.0" //version to be used on package "spark-testing-base"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, //Core library of spark
  "org.apache.spark" %% "spark-sql" % sparkVersion, //Core library of spark
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.1.1",
  //"org.elasticsearch" %% "elasticsearch-hadoop-20" % "7.1.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test, //Basic test library of scala
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBase % Test //Custom test library for spark
)

resolvers += Resolver.mavenLocal

//  SBT testing java options are too small to support running many of the tests due to the need to
//  launch Spark in local mode. Need to be increased
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")