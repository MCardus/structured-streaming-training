ThisBuild / scalaVersion := "2.12.11"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.2.7" % Test
)

lazy val structuredStreamingCourse = (project in file("."))
  .settings(
    name := "StructuredStreamingCourse",
    libraryDependencies ++= dependencies
  )
