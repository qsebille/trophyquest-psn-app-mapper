val scala3Version = "2.13.15"

Compile / mainClass := Some("com.trophyquest.PsnMapperMain")
Compile / run / fork := true

ThisBuild / javaOptions ++= Seq(
  "-DHADOOP_HOME=D:/tools/hadoop",
  "-Dhadoop.home.dir=D:/tools/hadoop"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "trophyquest-psn-app-mapper",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.postgresql" % "postgresql" % "42.7.3",
      "org.rogach" %% "scallop" % "5.3.0",
      "org.scalameta" %% "munit" % "1.0.0" % Test
    )
  )
