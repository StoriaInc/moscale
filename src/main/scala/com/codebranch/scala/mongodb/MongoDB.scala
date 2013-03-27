package com.codebranch.scala.mongodb


import com.{mongodb => MongoDriver}

case class ServerAddress
(host : String = MongoDriver.ServerAddress.defaultHost(),
 port : Int = MongoDriver.ServerAddress.defaultPort())

class MongoDB
{
	val mongoDriver = new MongoDriver.Mongo
}
