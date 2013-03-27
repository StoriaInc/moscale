package com.codebranch.scala.mongodb

import com.{mongodb => jmdb}
import jmdb.{Mongo => JMongo, _}
import handlers._
import collection.JavaConversions._
import java.util


class Mongo(val jMongo : JMongo)
{
  import Mongo._

	def this(uri : MongoURI) = this(new JMongo(uri))

	def this(addrs : Seq[jmdb.ServerAddress], options : MongoOptions) = this(new JMongo(seqAsJavaList(addrs), options))

	def this(addrs : Seq[jmdb.ServerAddress]) = this(new JMongo(seqAsJavaList(addrs), null))

	def this(addr : jmdb.ServerAddress, options : MongoOptions) = this(Seq(addr), options)

	def this(addr : jmdb.ServerAddress) = this(addr, null)

	def this(
		host : String = "localhost",
		port : Int = 27017,
		options : MongoOptions = null)
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
	def getCollection(databaseName : String, collectionName : String) = getDatabase(databaseName).getCollection(collectionName)


	/**
	 * Returns collection for type T. T should be annotated with CollectionEntity annotation.
	 * @param m Manifest[T].
	 * @tparam T entity type.
	 * @return collection of T.
	 */
	def getCollection[T <: Entity](implicit m : Manifest[T]) : Collection =
		(getDatabaseName[T], getCollectionName[T]) match {
			case (Some(dn), Some(cn)) => getCollection(dn, cn)
			case _ => throw new RuntimeException("Class %s should be annotated with @CollectionEntity annotation"
			                                     format (m.runtimeClass.getName))
		}

  @deprecated(message = "Use Collection.findOne() instead")
	def findOne[T <: Entity](query : Map[String, Value])(implicit m : Manifest[T], th : TypeHandler[T]) =
		getCollection[T].findOne[T](query)

  @deprecated(message = "Use Collection.find()")
  def find[T <: Entity](query : Map[String, Value])(implicit m : Manifest[T], th : TypeHandler[T]) =
    getCollection[T].find[T](query)

  @deprecated(message = "Use Collection.find() and cursor's skip and limit methods instead")
  def find[T <: Entity](query : Map[String, Value], limit: Int, offset: Int, sorter: DBObject)(implicit m : Manifest[T], th : TypeHandler[T]) =
    getCollection[T].find[T](query, limit, offset, sorter)

	@deprecated(message = "Use Collection.save()")
	def save[T <: Entity](entity : T)(implicit m : Manifest[T], th : TypeHandler[T]): WriteResult =
		getCollection[T].save(entity)

	@deprecated(message = "Use Collection.insert()")
  def insert[T <: Entity](entity : T)(implicit m : Manifest[T], th : TypeHandler[T]): WriteResult =
    getCollection[T].insert(entity)

	@deprecated(message = "Use Collection.delete()")
	def delete[T <: Entity](entity: T)(implicit m: Manifest[T], th: TypeHandler[T]) =
		getCollection[T].remove(entity)

//  @Deprecated
//  def update[T <: Entity](query: Map[String,Value], data: Map[String,Value], upsert: Boolean, multi:Boolean)(implicit m: Manifest[T], th: TypeHandler[T]) =
//    getCollection[T].update(query,data,upsert,multi)
}


object Mongo
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


object Query
{
  def expression(ev : (String, Value)*) = Value(Value.Map(ev:_*))

	def apply(ev : (String, Value)*) : Value.Map = Value.Map(ev:_*)

  def or(ev : Value*) = $or -> Value(Value.List(ev:_*))

  def in(ev : Value*) = $in -> Value(Value.List(ev:_*))

	def in(ev : List[Value]) = $in -> Value(ev)

  def all(ev : List[Value]) = $all -> Value(ev)

  def elemMatch(ev : (String, Value)*) = ($elemMatch -> Value(Value.Map(ev:_*)))

  def group(fields: String*)(ev : (String, Value)*): (String, Value) = {
    val b = ("_id" , expression(fields.map (f => (f -> Value("$" + f))):_*))
    $group ->  expression((b +: ev):_*)
  }
//    $group ->
//    expression(("_id" ->
//                 expression((fields.map(f => (f -> Value("$" + f)))):_*)), ev:_*)


  def max(fieldName : String) = expression($max -> Value("$" + fieldName))

  def max(v : Value) = expression($max -> v)

  def mMatch(ev: (String, Value)*) = $match -> expression(ev:_*)

  def set(ev : (String, Value)*) = $set -> expression(ev:_*)

  def gt(v : Value) = $gt -> v

  def lt(v : Value) = $lt -> v

  def gte(v : Value) = $gte -> v

  def lte(v : Value) = $lte -> v

  def exists(v : Boolean = true) = $exists -> Value(v)

  def notExists = exists(false)

  val $exists = "$exists"

  val $all = "$all"

  val $max = "$max"

  val $group = "$group"

  val $elemMatch = "$elemMatch"

  val $match = "$match"

  val $or = "$or"

  val $in = "$in"

  val $set = "$set"

  val $lt = "$lt"

  val $gt = "$gt"

  val $lte = "$lte"

  val $gte = "$gte"
}

trait MongoSupport
{
	implicit val mongo : Mongo
}
