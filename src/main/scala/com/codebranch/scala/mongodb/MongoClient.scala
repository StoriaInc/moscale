package com.codebranch.scala.mongodb

import com.{mongodb => jmdb}
import jmdb.{Mongo => JMongo,
MongoClient => JMongoClient,
MongoClientURI => JMongoClientURI, _}
import handlers._
import collection.JavaConversions._
import java.util


class MongoClient(val jMongo : JMongoClient)
{
  import MongoClient._

	def this(uri : JMongoClientURI) = this(new JMongoClient(uri))

	def this(addrs : Seq[jmdb.ServerAddress], options : MongoClientOptions) =
		this(new JMongoClient(seqAsJavaList(addrs), options))

	def this(addrs : Seq[jmdb.ServerAddress]) =
		this(new JMongoClient(seqAsJavaList(addrs)))

	def this(addr : jmdb.ServerAddress) = this(Seq(addr))

	def this(addr : jmdb.ServerAddress, options : MongoClientOptions) =
		this(Seq(addr), options)


	def this(
		host : String = "localhost",
		port : Int = 27017,
		options : MongoClientOptions = null)
	= this(new jmdb.ServerAddress(host, port), options)

	def getDatabase(name: String) = {
		val db = jMongo.getDB(name)
		new Database(db)
	}

	def setWriteConcern(wc : WriteConcern) = jMongo.setWriteConcern(wc)

	def getWriteConcern = jMongo.getWriteConcern


	/**
	 * Returns collection with name collectionName in database databaseName.
	 * @param databaseName database name.
	 * @param collectionName collection name.
	 * @return Returns collection with name collectionName in database databaseName.
	 */
	private def getCollection(databaseName : String, collectionName : String) = getDatabase(databaseName).getCollection(collectionName)


	/**
	 * Returns collection for type T. T should be annotated with CollectionEntity annotation.
	 * @param m Manifest[T].
	 * @tparam T entity type.
	 * @return collection of T.
	 */
	private[mongodb] def getCollection[T <: Entity](implicit m : Manifest[T]) : Collection =
		(getDatabaseName[T], getCollectionName[T]) match {
			case (Some(dn), Some(cn)) => getCollection(dn, cn)
			case _ => throw new RuntimeException("Class %s should be annotated with @CollectionEntity annotation"
			                                     format (m.runtimeClass.getName))
		}

//  @deprecated(message = "Use Collection.findOne() instead", since = "1.0")
//	def findOne[T <: Entity](query : Map[String, Value])(implicit m : Manifest[T], th : TypeHandler[T]) =
//		getCollection[T].findOne[T](query)
//
//  @deprecated(message = "Use Collection.find()", since = "1.0")
//  def find[T <: Entity](query : Map[String, Value])(implicit m : Manifest[T], th : TypeHandler[T]) =
//    getCollection[T].find[T](query)
//
//  @deprecated(message = "Use Collection.find() and cursor's skip and limit methods instead", since = "1.0")
//  def find[T <: Entity](query : Map[String, Value], limit: Int, offset: Int, sorter: DBObject)(implicit m : Manifest[T], th : TypeHandler[T]) =
//    getCollection[T].find[T](query, limit, offset, sorter)
//
//	@deprecated(message = "Use Collection.save()", since = "1.0")
//	def save[T <: Entity](entity : T)(implicit m : Manifest[T], th : TypeHandler[T]): WriteResult =
//		getCollection[T].save(entity)
//
//	@deprecated(message = "Use Collection.insert()", since = "1.0")
//  def insert[T <: Entity](entity : T)(implicit m : Manifest[T], th : TypeHandler[T]): WriteResult =
//    getCollection[T].insert(entity)
//
//	@deprecated(message = "Use Collection.delete()", since = "1.0")
//	def delete[T <: Entity](entity: T)(implicit m: Manifest[T], th: TypeHandler[T]) =
//		getCollection[T].remove(entity)
}


object MongoClient
{
	/**
	 * Returns name of the collection with elements of type T. T should be annotated with CollectionEntity annotation.
	 * @param m Manifest[T].
	 * @tparam T entity type.
	 * @return collection name.
	 */
	def getDatabaseName[T <: Entity](implicit m : Manifest[T]) : Option[String] =
		Option(m.runtimeClass.getAnnotation(classOf[CollectionEntity])).map(_.databaseName)


	/**
	 * Returns name of the database where elements of type T located. T should be annotated with CollectionEntity
	 * annotation.
	 */
	def getCollectionName[T <: Entity](implicit m : Manifest[T]) : Option[String] =
		Option(m.runtimeClass.getAnnotation(classOf[CollectionEntity])).map(_.collectionName)
}




trait MongoSupport
{
	implicit val mongo : MongoClient
}
