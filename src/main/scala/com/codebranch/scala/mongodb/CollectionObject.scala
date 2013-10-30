package com.codebranch.scala.mongodb

import com.mongodb.{BasicDBObject, WriteResult, DBObject}
import org.bson.types.ObjectId
import com.codebranch.scala.mongodb.handlers._

class CollectionObject[T <: Entity with EntityId](implicit manifest: Manifest[T], th: TypeHandler[T]) {

	private def collection(implicit mongo: MongoClient) =
		mongo.getCollection[T]

  def drop()(implicit mongo: MongoClient) {
    collection.drop()
  }

  def jColl(implicit mongo: MongoClient) = collection.jColl

	def find(query: DBObject, fields: DBObject = null)(implicit mongo: MongoClient): Cursor[T] = {
    Logger.debug("Find. Query = %s" format query)
    new Cursor[T](jColl.find(query, fields))
  }

	def findOne(query: DBObject, fields: DBObject = null)(implicit mongo: MongoClient): Option[T] = {
    Logger.debug("FindOne. Query = %s" format query.toString)
    val res = Option(jColl.findOne(query, fields, null: DBObject)).map(th.fromDBObject(_))
    Logger.debug("Found %s" format res.toString)
    res
  }

  def findAndModify(query: DBObject, update: DBObject, fields: DBObject = null, upsert: Boolean = false, returnNew: Boolean = true)(implicit mongo: MongoClient): DBObject =
    jColl.findAndModify(query, fields, null, false, update, returnNew, upsert)

  def findById(id: ObjectId, fields: DBObject = null)(implicit mongo: MongoClient): Option[T] =  {
    Logger.debug("FindById. Query = %s" format id.toString)
    val res =
      Option(jColl.findOne(DBObjectGen.compose(EntityId.Field.Id, id), fields, null: DBObject)).map(th.fromDBObject(_))
    Logger.debug("Found %s" format res.toString)
    res
  }

  def save(entity: T)(implicit mongo: MongoClient): WriteResult =
    entity match {
      case e: EntityValidator =>
        val invalidFields = e.validate
        if (invalidFields.isEmpty)
          jColl.save(toDBObject(entity))
        else
          throw new InvalidFields(invalidFields)
      case e =>
        jColl.save(toDBObject(entity))
    }

  def insert(entity: T)(implicit mongo: MongoClient): WriteResult = {
    entity match {
      case e: EntityValidator =>
        val invalidFields = e.validate
        if (invalidFields.isEmpty)
          jColl.insert(toDBObject(entity))
        else
          throw new InvalidFields(invalidFields)
      case e =>
        jColl.insert(toDBObject(entity))
    }
  }

  def update(query: DBObject, obj: DBObject, upsert: Boolean = false, multi: Boolean = false)(implicit mongo: MongoClient): WriteResult = {
    Logger.debug(s"update($query, $obj, multi = $multi, upsert = $upsert)")
    jColl.update(if (query == null) new BasicDBObject else query, obj, upsert, multi)
  }

  def remove(query: DBObject)(implicit mongo: MongoClient): WriteResult =
    jColl.remove(query)

  def remove(entity: T)(implicit mongo: MongoClient): WriteResult =
    entity match {
      case entityId: EntityId =>
        jColl.remove(new BasicDBObject(EntityId.Field.Id, entityId.id.get))
      case _ =>
        jColl.remove(toDBObject(entity))
    }

  def aggregate(first: DBObject, others: DBObject*)(implicit mongo: MongoClient) = {
    Logger.debug(s"aggregate($first, $others)")
    jColl.aggregate(first, others:_*)
  }

  def ensureIndex(keys: DBObject, name: Option[String] = None, unique: Boolean = false)(implicit mongo: MongoClient): Unit =
    collection.ensureIndex(keys, name, unique)

  def ensureIndex(key: String)(implicit mongo : MongoClient): Unit =
    collection.ensureIndex(key)

  private def toDBObject(obj: T) =
    Option(obj).map(th.toDBObject(_).asInstanceOf[DBObject]).orNull

  def count(implicit mongo: MongoClient) = collection.count
}
