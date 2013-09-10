import sbt._
import Keys._


object ApplicationBuild extends Build {
  def frumaticRepository(r: String) : Resolver =
    "Sonatype Nexus Repository Manager" at "http://nexus.frumatic.com/content/repositories/" + r
  val frumaticRepositorySnapshots = frumaticRepository("snapshots")
  val frumaticRepositoryReleases = frumaticRepository("releases")


	val scalaVer = "2.10.2"
	val appName       = "mongo"
  val isSnapshot = true
  val version = "1.7" + (if (isSnapshot) "-SNAPSHOT" else "")

	val buildSettings = Defaults.defaultSettings ++ Seq (
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
    "joda-time" % "joda-time" % "2.0",
    "org.joda" % "joda-convert" % "1.1",
    "org.mongodb" % "mongo-java-driver" % "2.11.0",
    "ch.qos.logback" % "logback-classic" % "1.0.7",
		"org.scala-lang" % "scala-reflect" % scalaVer,
    "com.basho.riak" % "riak-client" % "1.1.1",
    // testing libs
    "org.scalatest" %% "scalatest" % "1.9" % "test",
    "org.specs2" %% "specs2" % "1.14" % "test")


  val main = Project(
    appName,
    file("."),
		settings = buildSettings ++ Seq(libraryDependencies ++= appDependencies))
}
