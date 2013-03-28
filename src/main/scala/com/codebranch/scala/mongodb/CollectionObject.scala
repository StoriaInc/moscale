package com.codebranch.scala.mongodb


import com.mongodb.{WriteResult, DBObject}
import org.bson.types.ObjectId
import com.codebranch.scala.mongodb.handlers._



class CollectionObject[T <: Entity with EntityId](implicit manifest : Manifest[T], th : TypeHandler[T])
{
	//override def jColl(implicit mongo : MongoClient) = mongo.

  import EntityId.Field.Id
  import Value.{Map => VMap, _}

	def collection(implicit mongo : MongoClient) =
		mongo.getCollection[T]


  def drop()(implicit mongo : MongoClient) {
    collection.drop()
  }


	def find(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T]


	def find(query : DBObject, fields : DBObject)(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T](query, fields)


	def find(query : Value.Map, fields : Map[String, Boolean] = null)(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T](query, fields)


  @Deprecated
  def find
  (query: Map[String, Value], limit: Int, offset: Int, sorter: Map[String, Int])
  (implicit mongo: MongoClient)
  : Cursor[T] =
    collection.find[T](query, limit, offset, Value(sorter).dbObject.asInstanceOf[DBObject])


	def findOne
  (query : DBObject, fields : DBObject, order : DBObject)
  (implicit mongo : MongoClient)
  : Option[T] =
		collection.findOne[T](query, fields, order)


	def findOne
  (query : Map[String, Value], fields : Map[String, Boolean] = null, order : Map[String, Int] = null)
  (implicit mongo : MongoClient)
  : Option[T] =
    collection.findOne[T](query, fields, order)


  def findById(id: ObjectId)(implicit mongo: MongoClient) : Option[T] = collection.findById[T](id)


  def save(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.save(entity)


  def insert(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.insert(entity)


//  def update
//  (query : DBObject, obj : DBObject, upsert : Boolean = false, multi : Boolean = false)
//  (implicit mongo : MongoClient)
//  : WriteResult =
//    collection.update(query, obj, upsert, multi)


  def update
  (query : Value.Map, obj : Value.Map, upsert : Boolean = false, multi : Boolean = false)
  (implicit mongo : MongoClient)
  : WriteResult =
    collection.update(query, obj, upsert = upsert, multi = multi)


  def findAndModify
  (query : Value.Map,
   update : Value.Map,
   order : Map[String, Int] = null,
   fields : Map[String, Boolean] = null,
   upsert : Boolean = false,
   returnNew : Boolean = false)
  (implicit th : TypeHandler[T], mongo : MongoClient)
  : Option[T] =
    collection.findAndModify[T](query, update, order = order, fields = fields, upsert = upsert, returnNew = returnNew)


  def remove(query : DBObject)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(query)


  def remove(query : Value.Map)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(query)


  def remove(entity : T)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(entity)


  def aggregate(query: VMap, additionalQueries: VMap*)(implicit mongo: MongoClient) =
    collection.aggregate(query, additionalQueries:_*)


  def ensureIndex
  (keys : Map[String, Int], name : Option[String] = None, unique : Boolean = false)
  (implicit mongo : MongoClient)
  {
    collection.ensureIndex(keys, name, unique)
  }

  def ensureIndex
  (keys : Map[String, Int], options : Map[String, Value])
  (implicit mongo : MongoClient)
  {
    collection.ensureIndex(keys, options)
  }

  def ensureIndex
  (key : String)
  (implicit mongo : MongoClient)
  {
    collection.ensureIndex(key)
  }

  def count(implicit mongo : MongoClient) = collection.count

  def init()(implicit mongo : MongoClient) {}
}
