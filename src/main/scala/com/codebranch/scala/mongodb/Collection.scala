package com.codebranch.scala.mongodb

import com.{mongodb => jmdb}
import com.mongodb.{BasicDBObject, WriteResult, DBObject, AggregationOutput}
import scala.util.control.ControlThrowable
import handlers._

case class InvalidFields(errors: List[String]) extends ControlThrowable

class Collection(val jColl: jmdb.DBCollection) {
  val name = jColl.getName

  def count = jColl.count()

  def drop() {
    jColl.drop()
  }

  def findRaw(query: DBObject, fields: DBObject): RawCursor = {
    Logger.debug("Find. Query = %s" format query)
    new RawCursor(jColl.find(query, fields))
  }

  def findOneRaw(query: DBObject, fields: DBObject, order: DBObject): Option[DBObject] = {
    Logger.debug("FindOne. Query = %s" format query.toString)
    Option(jColl.findOne(query, fields, order))
  }

  def save(dbo : DBObject) : WriteResult = jColl.save(dbo)

  def insert(dbo : DBObject) : WriteResult =
    jColl.insert(dbo)


  def update(query: DBObject, obj: DBObject, upsert: Boolean, multi: Boolean) : WriteResult =
    jColl.update(if (query == null) new BasicDBObject else query, obj, upsert, multi)

  def findAndModify(query: DBObject, update: DBObject, order: DBObject, fields: DBObject, upsert: Boolean, returnNew: Boolean): DBObject = {
    jColl.findAndModify(query, fields, order, false, update, returnNew, upsert)
  }

  def remove(query : DBObject) : WriteResult =
    jColl.remove(query)

  def aggregate(query: DBObject, additionalQueries: DBObject*): AggregationOutput = {
    Logger.debug(s"aggregate. Query:${query}")
    jColl.aggregate(query, additionalQueries:_*)
  }

  def ensureIndex(name: String) {
    jColl.ensureIndex(name)
  }

  def ensureIndex(keys: DBObject, name: Option[String] = None, unique: Boolean = false) {
    jColl.ensureIndex(keys, name.orNull, unique)
  }

  def ensureIndex(keys: DBObject, options: DBObject) {
    jColl.ensureIndex(keys, options)
  }
}
