import xerial.sbt.Sonatype.sonatypeCentralHost

val ReleaseTag = """^release/([\d\.]+a?)$""".r

lazy val contributors = Seq(
 "pchlupacek" -> "Pavel Chlupáček"
  , "mraulim" -> "Milan Raulim"
  , "AdamChlupacek" -> "Adam Chlupáček"
)

lazy val commonSettings = Seq(
   organization := "com.spinoco",
   scalaVersion :=  "2.13.16",
   crossScalaVersions := Seq("2.12.20", "2.13.16"),
   scalacOptions := {
     val common = Seq(
       "-feature",
       "-deprecation",
       "-language:implicitConversions",
       "-language:higherKinds",
       "-language:existentials",
       "-language:postfixOps"
     )
     CrossVersion.partialVersion(scalaVersion.value) match {
       case Some((2, 12)) => common ++ Seq(
         "-Xfatal-warnings",
         "-Yno-adapted-args",
         "-Ywarn-value-discard",
         "-Ywarn-unused-import"
       )
       case Some((2, 13)) => common ++ Seq(
         "-Xfatal-warnings",
         "-Wvalue-discard",
         "-Wunused:imports"
       )
       case _ => common ++ Seq("-Xfatal-warnings")
     }
   },
   javaOptions += "-Djava.net.preferIPv4Stack=true",
   scalacOptions in (Compile, console) ~= {_.filterNot(opt => opt == "-Ywarn-unused-import" || opt == "-Wunused:imports")},
   scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
   libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.1.4" % "test"
    , "org.scalacheck" %% "scalacheck" % "1.14.3" % "test"
    , "org.scalatestplus" %% "scalacheck-1-14" % "3.1.4.0" % "test"
    , "co.fs2" %% "fs2-core" % "3.12.2"
    , "co.fs2" %% "fs2-io" % "3.12.2"
    , "com.spinoco" %% "protocol-kafka" % "0.5.1"

   ),
   scmInfo := Some(ScmInfo(url("https://github.com/Spinoco/fs2-kafka"), "git@github.com:Spinoco/fs2-kafka.git")),
   homepage := None,
   licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
   initialCommands := s"""
    import fs2._
    import fs2.util._
    import spinoco.fs2.kafka
    import spinoco.fs2.kafka._
    import spinoco.protocol.kafka._
    import scala.concurrent.duration._
  """
) ++ testSettings ++ scaladocSettings ++ publishingSettings ++ releaseSettings

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  publishArtifact in Test := true
)

lazy val scaladocSettings = Seq(
   scalacOptions in (Compile, doc) ++= Seq(
    "-doc-source-url", scmInfo.value.get.browseUrl + "/tree/master€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-implicits",
    "-implicits-show-all"
  ),
   scalacOptions in (Compile, doc) ~= { _ filterNot { _ == "-Xfatal-warnings" } },
   autoAPIMappings := true
)

lazy val publishingSettings = Seq(
  sonatypeCredentialHost := sonatypeCentralHost,
  publishTo := sonatypePublishToBundle.value,
  versionScheme := Some("early-semver"),
  organization := "com.spinoco",
  homepage := Some(url("https://github.com/spinoco/fs2-kafka")),
  licenses := List("MIT" -> url("http://opensource.org/licenses/MIT")),
  developers := {
    for ((username, name) <- contributors) yield
      Developer(
        username,
        name,
        "",
        url(s"https://github.com/$username")
      )
  }.toList,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/spinoco/fs2-kafka"),
      "scm:git@github.com:spinoco/fs2-kafka.git"
    )
  )
)

lazy val releaseSettings = Seq(
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value
)

lazy val `f2-kafka` =
  project.in(file("."))
  .settings(commonSettings)
  .settings(
    name := "fs2-kafka"
  )
 
 

