package com.codebranch.scala.mongodb


import com.{mongodb => jMongodb}

case class ServerAddress(host : String = jMongodb.ServerAddress.defaultHost(),
                         port : Int = jMongodb.ServerAddress.defaultPort())
