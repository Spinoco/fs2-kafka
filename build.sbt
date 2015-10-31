enablePlugins(GitVersioning)

organization := "spinoco"

name := "fs2-kafka"

// this is equivalent to declaring compatibility checks
git.baseVersion := "0.8"

val ReleaseTag = """^release/([\d\.]+a?)$""".r
git.gitTagToVersionNumber := {
  case ReleaseTag(version) => Some(version)
  case _ => None
}

git.formattedShaVersion := {
  val suffix = git.makeUncommittedSignifierSuffix(git.gitUncommittedChanges.value, git.uncommittedSignifier.value)

  git.gitHeadCommit.value map { _.substring(0, 7) } map { sha =>
    git.baseVersion.value + "-" + sha + suffix
  }
}

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7")

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-Yno-adapted-args"
)

scalacOptions in (Compile, doc) ++= Seq(
  "-implicits",
  "-implicits-show-all"
)

resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.sonatypeRepo("snapshots"))

libraryDependencies ++= Seq(
  "org.scalaz.stream" %% "scalaz-stream" % "0.8"
  , "org.scalacheck" %% "scalacheck" % "1.12.5" % "test"
  , "org.apache.kafka" % "kafka_2.11" % "0.8.2.2" % "test"
)



parallelExecution in Test := false

logBuffered in Test := false

testOptions in Test += Tests.Argument("-verbosity", "2")

autoAPIMappings := true

initialCommands := s"""
  import scalaz.stream._
  import fs2.kafka._

"""

