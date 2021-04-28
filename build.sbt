val Http4sVersion = "0.21.5"
val CirceVersion = "0.13.0"
val DoobieVersion = "0.9.0"
val LogbackVersion = "1.2.3"
val MysqlConnectorVersion = "8.0.21"
val ScalaTestVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    organization := "co.famly",
    name := "backend-challenge",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.13.2",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-server" % Http4sVersion,
      "org.http4s" %% "http4s-blaze-client" % Http4sVersion,
      "org.http4s" %% "http4s-circe" % Http4sVersion,
      "org.http4s" %% "http4s-dsl" % Http4sVersion,
      "io.circe" %% "circe-generic" % CirceVersion,
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-h2" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion,
      "mysql" % "mysql-connector-java" % MysqlConnectorVersion,
      "org.scalactic" %% "scalactic" % ScalaTestVersion,
      "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"
    ),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
  )

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-language:higherKinds",
  "-language:postfixOps",
  "-feature",
  "-Xlint:unused"
)
