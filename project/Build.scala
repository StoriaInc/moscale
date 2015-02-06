import sbt._
import Keys._


object ApplicationBuild extends Build {
  def frumaticRepository(r: String) : Resolver =
    "Sonatype Nexus Repository Manager" at "http://nexus.frumatic.com/content/repositories/" + r
  val frumaticRepositorySnapshots = frumaticRepository("snapshots")
  val frumaticRepositoryReleases = frumaticRepository("releases")


	val scalaVer = "2.11.5"
	val appName = "mongo"
  val isSnapshot = false
  val version = "1.23.1" + (if (isSnapshot) "-SNAPSHOT" else "")

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
    resolvers ++= Seq(
      "Local Maven Repository" at "file://" + Path.userHome.absolutePath + ".m2/repository",
       "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases" // for specs2 --> scalaz 
    ),
    publishTo := {
      if (isSnapshot)
        Some(frumaticRepositorySnapshots)
      else
        Some(frumaticRepositoryReleases)
    },
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
  )

  val appDependencies = Seq(
    "joda-time" % "joda-time" % "2.7",
    "org.joda" % "joda-convert" % "1.7",
    "org.mongodb" % "mongo-java-driver" % "2.13.0",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
		"org.scala-lang" % "scala-reflect" % scalaVer,
    // testing libs
    "org.specs2" %% "specs2" % "2.4.16" % "test"
  )


  val main = Project(
    appName,
    file("."),
		settings = buildSettings ++ Seq(libraryDependencies ++= appDependencies))
}
