package com.codebranch.scala.mongodb


import com.mongodb.{WriteResult, DBObject}
import org.bson.types.ObjectId
import com.codebranch.scala.mongodb.handlers._



class CollectionObject[T <: Entity with EntityId](implicit manifest : Manifest[T], th : TypeHandler[T])
{
	//override def jColl(implicit mongo : MongoClient) = mongo.

	def collection(implicit mongo : MongoClient) =
		mongo.getCollection[T]


  def drop()(implicit mongo : MongoClient) {
    collection.drop()
  }


//	def find(implicit mongo : MongoClient) : Cursor[T] =
//		collection.find[T]


	def find(query: DBObject)(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T](query, null)


	def find2(query: Value.Map)(implicit mongo : MongoClient) : Cursor[T] =
		collection.find[T](query)


	def findOne(query : DBObject)(implicit mongo : MongoClient): Option[T] =
		collection.findOne[T](query, null, null)


//	def findOne2(query : Map[String, Value])(implicit mongo : MongoClient): Option[T] =
//    collection.findOne[T](query, null, null)


  def findById(id: ObjectId)(implicit mongo: MongoClient) : Option[T] =
    findOne(DBObjectGen.compose(EntityId.Field.Id, id))

  def save(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.save(entity)

  def insert(entity : T)(implicit mongo : MongoClient) : WriteResult = collection.insert(entity)


  def update
  (query : DBObject, obj : DBObject, upsert : Boolean = false, multi : Boolean = false)
  (implicit mongo : MongoClient)
  : WriteResult =
    collection.update(query, obj, upsert, multi)


//  def update
//  (query : Value.Map, obj : Value.Map, upsert : Boolean = false, multi : Boolean = false)
//  (implicit mongo : MongoClient)
//  : WriteResult =
//    collection.update(query, obj, upsert = upsert, multi = multi)


//  def findAndModify(query : Value.Map, update : Value.Map, order : Map[String, Int] = null,
//    fields : Map[String, Boolean] = null, upsert : Boolean = false, returnNew : Boolean = false)
//    (implicit th : TypeHandler[T], mongo : MongoClient): Option[T] =
//    collection.findAndModify[T](query, update, order = order, fields = fields, upsert = upsert, returnNew = returnNew)


  def remove(query : DBObject)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(query)


//  def remove(query : Value.Map)(implicit mongo : MongoClient) : WriteResult =
//    collection.remove(query)


  def remove(entity : T)(implicit mongo : MongoClient) : WriteResult =
    collection.remove(entity)


//  def aggregate(query: VMap, additionalQueries: VMap*)(implicit mongo: MongoClient) =
//    collection.aggregate(query, additionalQueries:_*)


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
