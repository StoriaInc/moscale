import sbt._
import Keys._


object ApplicationBuild extends Build {
  def frumaticRepository(r: String) : Resolver =
    "Sonatype Nexus Repository Manager" at "http://nexus.frumatic.com/content/repositories/" + r
  val frumaticRepositorySnapshots = frumaticRepository("snapshots")
  val frumaticRepositoryReleases = frumaticRepository("releases")


  val scalaVer = "2.10.3"
  val appName       = "mongo"
  val isSnapshot = true
  val version = "1.15" + (if (isSnapshot) "-SNAPSHOT" else "")

  val scalaStyleSettings = org.scalastyle.sbt.ScalastylePlugin.Settings

  val buildSettings = Defaults.defaultSettings ++ scalaStyleSettings ++ Seq (
    organization := "codebranch",
    Keys.version := version,
    scalaVersion := scalaVer,
    scalacOptions in ThisBuild ++= Seq(
      "-feature",
      "-language:postfixOps",
      "-deprecation"),
    retrieveManaged := true,
    testOptions in Test := Nil,
    resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + ".m2/repository",
    publishTo := {
      if (isSnapshot)
        Some(frumaticRepositorySnapshots)
      else
        Some(frumaticRepositoryReleases)
    },
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
  )

  val appDependencies = Seq(
    "joda-time" % "joda-time" % "2.3",
    "org.joda" % "joda-convert" % "1.5",
    "org.mongodb" % "mongo-java-driver" % "2.11.3",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.scala-lang" % "scala-reflect" % scalaVer,
    // testing libs
    "org.specs2" %% "specs2" % "2.2.3" % "test"
  )


  val main = Project(
    appName,
    file("."),
    settings = buildSettings ++ Seq(libraryDependencies ++= appDependencies))
}
