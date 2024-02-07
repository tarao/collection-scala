import sbt._
import Keys._

val scala212 = "2.12.16"
val scala213 = "2.13.8"
val scala3 = "3.2.0"
val primaryScalaVersion = scala213
val supportedScalaVersions = Seq(scala3, scala213, scala212)

lazy val taraocollection = (project in file(".")).
  settings(
    name := "collection",
    organization := "com.github.tarao",
    version := "1.0.0",
    scalaVersion := primaryScalaVersion,
    crossScalaVersions := supportedScalaVersions,

    // Depenency
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest-funspec" % "3.2.18" % "test",
      "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.13" % "test",
    ),

    // Compilation
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature"
    ),

    // Documentation
    Compile / doc / scalacOptions ++= Seq(
      "-sourcepath", baseDirectory.value.getAbsolutePath,
      "-doc-source-url", "https://github.com/tarao/collection-scala/blob/master€{FILE_PATH}.scala",
      "-implicits",
      "-groups"
    ),
    autoAPIMappings := true,

    // Publishing
    publishMavenStyle := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    pomExtra := (
      <url>https://github.com/tarao/collection-scala</url>
      <licenses>
        <license>
          <name>MIT License</name>
          <url>http://www.opensource.org/licenses/mit-license.php</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:tarao/collection-scala.git</url>
        <connection>scm:git:git@github.com:tarao/collection-scala.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tarao</id>
          <name>INA Lintaro</name>
          <url>https://github.com/tarao/</url>
        </developer>
      </developers>)
  )
