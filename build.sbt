name := "SparkCourse"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= {

  val sparkVer = "2.3.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources()
  )
}