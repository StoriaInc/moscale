import sbt._
import Keys._


object ApplicationBuild extends Build
{
	val scalaVer = "2.10.0"

	val appName         = "mongo"
	val buildSettings = Defaults.defaultSettings ++ Seq (
		organization := "codebranch",
		version      := "1.0-SNAPSHOT",
		scalaVersion := scalaVer,
    scalacOptions in ThisBuild ++= Seq(
      "-feature",
      "-language:postfixOps",
      "-deprecation"),
   	retrieveManaged := true,
		testOptions in Test := Nil,
		resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
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
			settings = buildSettings ++
			Seq(libraryDependencies ++= appDependencies))
}
