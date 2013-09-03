package com.codebranch.scala.mongodb


import org.specs2.mutable._

import java.lang.{Integer => JInteger}
import handlers._
import Value.asValue
import org.joda.time.DateTime
import com.mongodb.{MongoClientURI, BasicDBObject, DBObject}
import ch.qos.logback.core.helpers.ThrowableToStringArray


@CollectionEntity(databaseName = "test", collectionName = "TestEntity")
class TestEntity extends Entity with EntityId {
	val intF = Field[Int]("intF")
	val strF = Field[String]("strF", Some(""))
	val enumF = Field[Direction.Value]("enumF", Some(Direction.East))

	override def equals(that: Any) = that match {
		case that: TestEntity => this.id == that.id
		case _ => false
	}

	override def hashCode = strF.toOption.##
}



class TestEntityWithDefaults extends Entity {
	val intF = Field[Int]("intF", Some(10))
	val strF = Field[String]("strF", Some("default"))
}



class TestComplexEntityWithDefaults extends Entity {
	val intF = Field[Int]("intF", Some(10))
	val entityF = Field[TestEntityWithDefaults]("entityF", Some(new TestEntityWithDefaults))
}



@CollectionEntity(databaseName = "test", collectionName = "TestComplexEntity")
class TestComplexEntity extends Entity with EntityId {
	val name = Field[String]("name")
	val children = Field[List[TestEntity]]("children")
	val leaf = Field[TestEntity]("leaf")
//	val refF = Field[Reference[TestEntity]]("child")
}
@CollectionEntity(databaseName = "test", collectionName = "TestEntityWithValidator")
class TestEntityWithValidator extends Entity with EntityValidator {
	val requiredField = Field[String]("required", Some("a"))

  validator("validation error") {
    false
  }
}



object Direction extends Enumeration {
	val South, West, East, North = Value
}



class TestMongoDB extends Specification with BeforeAfter {
	implicit var mongo: MongoClient = _
	var testDb: Database = _
	var testCol: Collection = _

  sequential

	implicit def toOption[T](v: T): Option[T] = Some(v)

	def before {
		mongo = new MongoClient(new MongoClientURI("mongodb://localhost:27017/test"))
    mongo.getDatabase("test").drop()
	}

	def after {}

	"Entity" should {
		"convert to DBObject" in {
			val e = new TestEntity
			e.intF := Some(10)
			e.strF := "test"
			e.enumF := Direction.West
			val o = e.toDBObject
			o.get("intF").asInstanceOf[JInteger].intValue must beEqualTo(10)
			o.get("strF").asInstanceOf[String] must beEqualTo("test")
			o.get("enumF").asInstanceOf[String] must beEqualTo("West")
		}


//		"convert Entity with nulls" in {
//			val e = new TestEntity
//			(e.strF := (null: String)) must throwA[IllegalArgumentException]
//		}


		"convert Entity" in {
			val e = new TestEntity
			e.intF := Some(10)
			e.strF := "test"

			val e2 = new TestEntity
			e2.fromDBObject(e.toDBObject)

			e2.intF.get must beEqualTo(10)
			e2.strF.get must beEqualTo("test")
		}


		"convert complex Entity" in {
			import Field._
			val e = new TestComplexEntity
			e.name := Some("TestComplexEntity")
			e.leaf := Some(new TestEntity {
				className := classOf[TestEntity].getName
				strF := "test"
				intF := Some(10)
			})
			val o = e.toDBObject
			val e2 = new TestComplexEntity
			e2.fromDBObject(o)
      e2.name.get.get must beEqualTo("TestComplexEntity")
      e2.leaf.get.get.intF.get must beEqualTo(10)
//			e2.name.get foreach (_ must beEqualTo("TestComplexEntity"))
//			e2.leaf.get foreach (_.intF.get.foreach(_ must beEqualTo(10)))
		}


		"read polimorphic Entity" in {
			val o = (new TestComplexEntity).toDBObject
			val e = Entity(o).asInstanceOf[TestComplexEntity]
		}


		"accept := :=> field operators" in {
			val e = new TestEntity
			e.intF := Some(10)
			e.strF := ""
//			e.intF :=> {
//				case Some(x) => Some(x + 1)
//				case None => None
//			}
			val ov = e.intF.get
		}




		"Mongo query test" in {
			val te = new TestEntity
			te.intF := Option(3)
			te.strF := "uniq"

			val coll = mongo.getCollection[TestEntity]
			coll drop()
			coll save (te)

			Stream from 1 take 3 foreach (x => coll save (new TestEntity))

			"Collection.find() is returning all records" <==>
			(coll.find[TestEntity].getStream.length must beEqualTo(4))

			"There is exactly one such entity!" <==>
			(coll.find[TestEntity](te.toDBObject, null: DBObject).getStream.length must beEqualTo(1))

			"Only one test entity have strF = uniq" <==>
			(coll.find[TestEntity](Value.Map("strF" -> "uniq")).getStream.length must beEqualTo(1))

			"Only one test entity have intF = 3" <==>
			(coll.find[TestEntity](Value.Map("intF" -> 3)).getStream.length must beEqualTo(1))

			coll.findOne[TestEntity](te.toDBObject, null: DBObject, null: DBObject).isEmpty must beFalse
			coll.findOne[TestEntity](Value.Map("_id" -> te.id.toOption)).isEmpty must beFalse
		}


//		"Reference test" in {
//			val te = new TestEntity
//			te.intF := Option(3)
//			te.strF := "uniq"
//			val ce = new TestComplexEntity
//			ce.refF := Some(Reference(te))
//			mongo.getCollection[TestEntity].save(te)
//
//			val obj = ce.toDBObject
//			val e2 = new TestComplexEntity
//			e2.fromDBObject(obj)
//      e2.refF.get.fetch must beEqualTo(Some(te))
////			e2.refF.get.get.fetch.get must beEqualTo(te)
//		}

		"Equality test" in {
			val e = new TestEntity
			val e2 = new TestEntity
			e.strF := "test"
			e2.strF := "test"
			e.strF.get must beEqualTo ("test")
			e.strF.get must beEqualTo ("test")
			e2.strF must beEqualTo (e.strF)
		}

		"Enumeration test" in {
			val th = implicitly[TypeHandler[Direction.Value]]
			val s = Direction.South
			val obj = th.toDBObject(s)
//			Logger.debug(obj.toString)
			val s2 = th.fromDBObject(obj)
//			Logger.debug(s2.toString)
			s must beEqualTo(s2)
		}


		"Mongo getCollection by entity type" in {
			val collection = mongo.getCollection[TestComplexEntity]
		}


		"Create - findOne - remove" in {
			val te = new TestEntity
			val collection = mongo.getCollection[TestEntity]
			collection.save(te)
			val fte = collection.findOne[TestEntity](
				Map(EntityId.Field.Id -> Value(te.id.get)))
			"Saved TestEntity was found" <==> (fte.isEmpty must beFalse)
			val wr = collection.remove[TestEntity](fte.get)
			"Entity was successfully removed" <==> (wr.getLastError.ok() must beTrue)
		}

		"Conversion Joda DateTime" in {
			val v = new DateTime
			val th = implicitly[TypeHandler[DateTime]]
			val dbo = th.toDBObject(v)
			val v2: DateTime = th.fromDBObject(dbo)

			"Dates are equal" <==>
			(v.compareTo(v2) must beEqualTo(0))
		}

		"Converting partially from dbObject" in {
			val e = new TestEntityWithDefaults
			val dbo = new BasicDBObject()
			dbo.put("className", "com.codebranch.scala.mongodb.TestEntityWithDefaults")
			dbo.put("strF", "newValue")
			e.fromDBObject(dbo, partial = true)
			"intF does contain default toOption" <==>
			(e.intF.get must beEqualTo(10))
			"strF does contain a new toOption" <==>
			(e.strF.get must beEqualTo("newValue"))
		}

		"Converting complex entity partially from DBObject" in {
			Logger.debug("Complex entity test start")
			val e = new TestComplexEntityWithDefaults
			val dbo = new BasicDBObject()
			dbo.put("className", "com.codebranch.scala.mongodb.TestComplexEntityWithDefaults")
			val innerDbo = new BasicDBObject()
			innerDbo.put("className", "com.codebranch.scala.mongodb.TestEntityWithDefaults")
			innerDbo.put("strF", "newValue")
			dbo.put("entityF", innerDbo)
			e.fromDBObject(dbo, partial = true)
//			Logger.debug(dbo.toString)

			"intF does contain default toOption" <==>
			(e.intF.get must beEqualTo(10))

//			"intF of inner entity does contain default toOption" <==>
//			(e.entityF.toOption.intF.get must beEqualTo(Some(10)))
		}

		"Do field validation" in {
			val e = new TestEntityWithValidator
			e.validate.size must beGreaterThan(0)
		}

		"Throw validation exception" in {
			val e = new TestEntityWithValidator
			val collection = mongo.getCollection[TestEntityWithValidator]
			collection.insert(e) must throwA[InvalidFields]
		}
	}
}
