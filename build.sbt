import sbt._
import Keys._

lazy val taraocollection = (project in file(".")).
  settings(
    name := "collection",
    organization := "com.github.tarao",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.12.15",
    crossScalaVersions := Seq("2.13.6", "2.12.15"),

    // Depenency
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest-funspec" % "3.2.9" % "test",
      "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.9" % "test",
    ),

    // Compilation
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature"
    ),

    // Documentation
    scalacOptions in (Compile, doc) ++= Seq(
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
