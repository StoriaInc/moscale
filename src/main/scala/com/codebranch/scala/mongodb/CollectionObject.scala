package com.codebranch.scala.mongodb


import com.mongodb.{WriteResult, DBObject}
import org.bson.types.ObjectId
import com.codebranch.scala.mongodb.handlers._



class CollectionObject[T <: Entity with EntityId](implicit manifest : Manifest[T], th : TypeHandler[T]) {

	def collection(implicit mongo : MongoClient) =
		mongo.getCollection[T]

  def drop()(implicit mongo : MongoClient) {
    collection.drop()
  }

	def find(query: DBObject)(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T](query, null)

	def findOne(query : DBObject)(implicit mongo : MongoClient): Option[T] =
		collection.findOne[T](query, null, null)

  def findById(id: ObjectId)(implicit mongo: MongoClient) : Option[T] =
    findOne(DBObjectGen.compose(EntityId.Field.Id, id))

  def save(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.save(entity)

  def insert(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.insert(entity)

  def update(query : DBObject, obj : DBObject, upsert : Boolean = false, multi : Boolean = false)(implicit mongo : MongoClient): WriteResult = {
    Logger.debug("UPDATE QUERY:")
    Logger.debug(query.toString)
    Logger.debug("OBJECT")
    Logger.debug(obj.toString)
    collection.update(query, obj, upsert, multi)
  }

  def remove(query : DBObject)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(query)

  def remove(entity : T)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(entity)

  def aggregate(first: DBObject, others: DBObject*)(implicit mongo: MongoClient) = {
    Logger.debug("AGGREGATE QUERY:")
    Logger.debug(first.toString)
    val result = collection.aggregate(first, others:_*)
    others.foreach(query => Logger.debug(query.toString))
    result
  }


  def ensureIndex(keys : Map[String, Int], name : Option[String] = None, unique : Boolean = false)(implicit mongo : MongoClient): Unit = {
    collection.ensureIndex(keys, name, unique)
  }

  def ensureIndex(keys : Map[String, Int], options : Map[String, Value])(implicit mongo : MongoClient): Unit = {
    collection.ensureIndex(keys, options)
  }

  def ensureIndex(key : String)(implicit mongo : MongoClient): Unit = {
    collection.ensureIndex(key)
  }

  def count(implicit mongo : MongoClient) = collection.count
}
