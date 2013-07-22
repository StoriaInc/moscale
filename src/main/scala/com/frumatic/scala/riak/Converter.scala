package com.frumatic.scala.riak

import com.mongodb.util.JSON
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.cap.VClock

abstract class Converter[T] {
  def toDomain(json: String, vclock: VClock): T
  def fromDomain(domainObject: T, vclock: VClock): IRiakObject
}

class RiakEntityConverter[T <: RiakEntity](implicit m: Manifest[T]) extends Converter[T] {

  private val th = RiakEntity.defaultTypeHandler[T]

  // TODO remove code duplication
  val bucketName = {
    val annotation = Option(m.runtimeClass.getAnnotation(classOf[BucketSettings]))
    annotation.map(_.bucketName).getOrElse(m.runtimeClass.getSimpleName)
  }

  def toDomain(json: String, vclock: VClock): T = {
    val domainObj = th.fromDBObject(JSON.parse(json))
    domainObj.vclock = Option(vclock)
    domainObj
  }

  def fromDomain(domainObj: T, vclock: VClock): IRiakObject = {
    val objBuilder = RiakObjectBuilder.newBuilder(bucketName, domainObj.id.get.toString)
      .withValue(th.toDBObject(domainObj).toString)
    domainObj.buildBinIndices.foreach{case (k, v) => objBuilder.addIndex(k, v)}
    domainObj.buildIntIndices.foreach{case (k, v) => objBuilder.addIndex(k, v)}
    domainObj.buildLinks.foreach(link => objBuilder.addLink(link._1, link._2, link._3))
    if (vclock == null && domainObj.vclock.isEmpty) {
      objBuilder.build()
    } else {
      objBuilder.withVClock(Option(vclock).getOrElse(domainObj.vclock.get)).build()
    }
  }

//  def fromDomainPbc(domainObj: T, vclock: Array[Byte]): RiakObject = {
//    val result =
//      if (vclock == null && domainObj.vclock.isEmpty)
//        new RiakObject(bucketName, domainObj.id.get.toString, th.toDBObject(domainObj).toString)
//      else
//        new RiakObject(ByteString.copyFrom(vclock), ByteString.copyFromUtf8(bucketName),
//          ByteString.copyFromUtf8(domainObj.id.get.toString), ByteString.copyFromUtf8(th.toDBObject(domainObj).toString))
//    domainObj.buildBinIndices.foreach{case (k, v) => result.addIndex(k + "_bin", v)}
//    domainObj.buildIntIndices.foreach{case (k, v) => result.addIndex(k + "_int", v)}
//    domainObj.buildLinks.foreach(link => result.addLink(link._1, link._2, link._3))
//    result
//  }

//  def fromDomainHttp(domainObj: T, vclock: VClock, client: JRiakClient): RiakObject = {
//    import scala.collection.JavaConversions._
//    val value = th.toDBObject(domainObj).toString.getBytes(Charset.forName("UTF-8"))
//    val links = domainObj.buildLinks.map(l => new RiakLink(l._1, l._2, l._3)).toList
//    val clock = Option(Option(vclock).getOrElse(domainObj.vclock.getOrElse(null)))
//    val indices = new util.ArrayList[RiakIndex[_]]
//    domainObj.buildBinIndices.foreach{case (k, v) => indices.add(new BinIndex(k + "_bin", v))}
//    domainObj.buildIntIndices.foreach{case (k, v) => indices.add(new IntIndex(k + "_int", v))}
//    new RiakObject(client, bucketName, domainObj.id.get.toString, value, "application/json", links,
//      new util.HashMap[String, String], clock.map(_.asString()).getOrElse(null), null, null, indices, false)
//  }

}
