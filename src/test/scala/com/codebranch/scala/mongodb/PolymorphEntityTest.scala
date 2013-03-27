package com.codebranch.scala.mongodb

import org.specs2.mutable._
import com.codebranch.scala.mongodb.handlers._



//class TestEntity extends Entity {
//  val intF = OptionalField[Int]("intF")
//}


class PolymorphEntityTest extends Specification {
	"EntityTypeHandler" should {
		"convert subclasses of generic entity" in {
			val e = new TestEntity
			e.intF := Some(10)
			val th = entityTypeHandler[Entity]
			val e2 = th.fromDBObject(e.toDBObject)
			e2 must beAnInstanceOf[TestEntity]
		}
	}
}
