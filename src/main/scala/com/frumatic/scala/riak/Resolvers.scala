package com.frumatic.scala.riak

import com.basho.riak.client.IRiakObject

trait Resolver[T <: RiakEntity] {

  def resolve(siblings: Seq[IRiakObject]): IRiakObject
}

class ChooseHeadResolver[T <: RiakEntity] extends Resolver[T] {

  def resolve(siblings: Seq[IRiakObject]): IRiakObject = {
    if (siblings.size > 1) Logger.debug(s"Resolving between ${siblings.length} siblings")
    siblings.head
  }

}