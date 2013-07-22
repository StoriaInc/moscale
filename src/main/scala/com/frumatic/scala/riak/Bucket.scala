package com.frumatic.scala.riak

import com.mongodb.DBObject
import com.mongodb.util.JSON
import org.bson.types.ObjectId
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.builders.BucketPropertiesBuilder
import com.basho.riak.client.http.response.RiakIORuntimeException
import com.basho.riak.client.query.indexes.BinIndex
import com.basho.riak.client.raw.query.MapReduceSpec
import com.basho.riak.client.raw.RawClient
import com.basho.riak.client.raw.query.indexes.BinValueQuery

class Bucket[T <: RiakEntity](val name: String, httpClient: RawClient, pbClient: RawClient,
                              converter: Converter[T], mutator: Mutator[T], resolver: Resolver[T])(implicit m: Manifest[T]) {

  import com.codebranch.scala.mongodb.handlers.entityTypeHandler

  def store(entity: T): Unit = {
    val errorFields = entity.validate
    if (errorFields.isEmpty) {
      val (existing, vclock) = retryOnFail {
        val result = pbClient.fetch(name, entity.id.get.toString)
        resolveInt(result.getRiakObjects) -> result.getVclock
      }
      val mutatedEntity = existing match {
        case Some(oldValue) =>
          mutator.mutate(converter.toDomain(oldValue.getValueAsString, vclock), entity)
        case None =>
          entity
      }
      retryOnFail {
        pbClient.store(converter.fromDomain(mutatedEntity, vclock))
      }
    } else
      throw new RiakInvalidFields(errorFields)
  }

  def storeUnsafe(entity: T): Unit = {
    val errorFields = entity.validate
    if (errorFields.isEmpty) {
      retryOnFail {
        pbClient.store(converter.fromDomain(entity, null))
      }
    } else
      throw new RiakInvalidFields(errorFields)
  }

  def fetch(id: String): Option[T] = {
    val result = retryOnFail {
      resolveInt(pbClient.fetch(name, id).getRiakObjects)
    }
    result.map(r => converter.toDomain(r.getValueAsString, r.getVClock))
  }

  def fetch(id: ObjectId): Option[T] =
    fetch(id.toString)

  def fetchByIndex(indexName: String, value: String): Stream[T] = {
    val valueQuery = new BinValueQuery(BinIndex.named(indexName), name, value)
    val result = retryOnFail {
      pbClient.fetchIndex(valueQuery)
    }
    streamKeyFetches(result.iterator())
  }

  def fetchMapReduce(mrb: MapReduceBuilder[T]): List[T] = {
    import scala.collection.JavaConversions._
    val result = pbClient.mapReduce(new MapReduceSpec(mrb.build))
    val resultJson = JSON.parse(result.getResultRaw).asInstanceOf[DBObject]
    Logger.debug("Riak response: " + resultJson)
    resultJson.toMap.values().map(o => converter.toDomain(o.toString, null)).toList
  }

  def streamKeyFetches(keysIterator: java.util.Iterator[String]): Stream[T] = {
    if (keysIterator.hasNext)
      fetch(keysIterator.next()).get #:: streamKeyFetches(keysIterator)
    else
      Stream.empty
  }

  def delete(entity: T): Unit = {
    delete(entity.id.get)
  }

  def delete(entityId: ObjectId): Unit = {
    retryOnFail {
      pbClient.delete(name, entityId.toString)
    }
  }

  def drop(): Unit = {
    import scala.collection.JavaConversions._
    try {
      pbClient.listKeys(name).foreach(key => pbClient.delete(name, key))
    } catch {
      case e: RiakIORuntimeException =>
    }
  }

  def updateProperties(allowSiblings: BooleanOption = BooleanOption(None), lastWriteWins: BooleanOption = BooleanOption(None),
                       nVal: IntOption = IntOption(None), r: IntOption = IntOption(None), w: IntOption = IntOption(None),
                       rw: IntOption = IntOption(None), dw: IntOption = IntOption(None), pr: IntOption = IntOption(None),
                       pw: IntOption = IntOption(None), basicQuorum: BooleanOption = BooleanOption(None),
                       notFoundOk: BooleanOption = BooleanOption(None)): Unit = {
    if (allowSiblings.value.isDefined && lastWriteWins.value.isDefined)
      require(allowSiblings.value.get ^ lastWriteWins.value.get)
    val args = List(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)
    val updateBucket = args.exists(_.value.isDefined)
    if (updateBucket) {
      val bp = new BucketPropertiesBuilder()
      allowSiblings.value.foreach(bp.allowSiblings)
      lastWriteWins.value.foreach(bp.lastWriteWins)
      nVal.value.foreach(bp.nVal)
      r.value.foreach(bp.r)
      w.value.foreach(bp.w)
      rw.value.foreach(bp.rw)
      dw.value.foreach(bp.dw)
      pr.value.foreach(bp.pr)
      pw.value.foreach(bp.pw)
      basicQuorum.value.foreach(bp.basicQuorum)
      notFoundOk.value.foreach(bp.notFoundOK)
      httpClient.updateBucket(name, bp.build())
    }
  }

  private def retryOnFail[K](action: => K, attempts: Int = 3): K = {
    try {
      action
    } catch {
      case e: Throwable if attempts > 0 => retryOnFail(action, attempts - 1)
    }
  }

  private def resolveInt(objs: Array[IRiakObject]): Option[IRiakObject] = {
    if (objs == null || objs.isEmpty) None
    else Some(resolver.resolve(objs.toSeq))
  }

}

abstract class SetBucketOption(val value: Option[Any])
case class BooleanOption(override val value: Option[Boolean]) extends SetBucketOption(value)
case class IntOption(override val value: Option[Int]) extends SetBucketOption(value)
