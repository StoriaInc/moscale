package com.frumatic.scala.riak

import com.basho.riak.client.cap.VClock
import com.codebranch.scala.mongodb._
import com.codebranch.scala.mongodb.InvalidFields

class RiakEntity extends Entity with EntityId {

  private val intIndices = new collection.mutable.HashMap[String, () => Long]
  private val binIndices = new collection.mutable.HashMap[String, () => String]
  private[riak] var vclock: Option[VClock] = None

  protected def intIndex(name: String)(validator: => Long): Unit =
    intIndices += name -> {() => validator}

  protected def binIndex(name: String)(validator: => String): Unit =
    binIndices += name -> {() => validator}

  def buildIntIndices: Map[String, Long] =
    intIndices.mapValues(vf => vf()).toMap

  def buildBinIndices: Map[String, String] =
    binIndices.mapValues(vf => vf()).toMap

  // triples encode bucket - key - tag
  def buildLinks: Seq[(String, String, String)] =
    Seq.empty

  def validate: List[String] = {
    this match {
      case e: FieldValidator =>
        e.validate.flatMap{case (k, v) => v}.toList
      case _ =>
        Nil
    }
  }
}

object RiakEntity {
  def defaultTypeHandler[T <: RiakEntity](implicit m: Manifest[T]) = {
    new EntityTypeHandler[T]
  }
}

class RiakInvalidFields(errorList: List[String]) extends InvalidFields(errorList.map(v => v -> Seq(v)).toMap) {

}