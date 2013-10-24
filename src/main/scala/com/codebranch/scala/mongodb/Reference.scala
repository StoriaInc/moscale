package com.codebranch.scala.mongodb


import com.mongodb.{BasicDBObject, DBObject}
import org.bson.types.ObjectId
import handlers._


//object Reference
//{
//  def apply[T <: EntityId](entity: T)(implicit th: TypeHandler[T],m: Manifest[T]) = {
//    require(entity.id.isDefined, "Entity id couldn't be None")
//    new Reference[T](entity.id.toOption.get)
//  }
//
//
//  def fromDBObject[T <: EntityId](sref: DBObject)(implicit th: TypeHandler[T],
//                                                          m: Manifest[T]) = {
//    val id = sref.get("$id").asInstanceOf[ObjectId]
//
//    if (null == id)
//      throw new RuntimeException("Collection objectId must be defined!")
//
//    new Reference[T](id)
//  }
//}
//
//
//class Reference[T <: EntityId]
//(val id: ObjectId)
//(implicit th: TypeHandler[T], m: Manifest[T])
//{
//  private var _value: Option[T] = None
//  val ref = m.runtimeClass.getAnnotation(classOf[CollectionEntity]).collectionName
//  val db = m.runtimeClass.getAnnotation(classOf[CollectionEntity]).databaseName
//
//
//  def toDBObject = {
//    if (!m.runtimeClass.isAnnotationPresent(classOf[CollectionEntity]))
//      throw new RuntimeException("Class %s should be annotated with @CollectionEntity annotation"
//                                 format (m.runtimeClass.getName))
//
//    val sref = new BasicDBObject()
//    sref.put("$ref", ref)
//    sref.put("$id", id)
//    sref.put("$db", db)
//    sref
//  }
//
//
//  def fetch (implicit mongo: MongoClient): Option[T] = {
//    if (null == ref || null == id || null == db)
//      throw new RuntimeException("Collection name and objId must be defined!")
//
//    _value match {
//      case None =>
//        _value = mongo.getCollection(db, ref).findById[T](id)
//        _value
//      case x => x
//    }
//  }
//}
