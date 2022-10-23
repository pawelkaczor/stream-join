ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

organization := "pl.newicom.akka"

name := "stream-join"

scalaVersion := "2.13.10"

publishMavenStyle := true
homepage := Some(new URL("http://github.com/pawelkaczor/stream-join"))
licenses := ("Apache2", new URL("http://raw.githubusercontent.com/pawelkaczor/stream-join/master/LICENSE")) :: Nil
publishTo := sonatypePublishToBundle.value

sonatypeProfileName := "pl.newicom"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"  % "2.6.20",
  "com.typesafe.akka" %% "akka-testkit"  % "2.6.20" % Test,
  "org.scalatest" %% "scalatest"       % "3.2.12" % Test
)

Publish.settings
