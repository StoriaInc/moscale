package com.codebranch.scala.mongodb


import org.specs2.mutable.{BeforeAfter, Specification}
import com.mongodb.MongoClientURI
import handlers._


/**
 * User: alexey
 * Date: 5/31/13
 * Time: 5:17 PM
 */
class TypeHandlerTest extends Specification with BeforeAfter {

  implicit var mongo: MongoClient = _


  def before {
    mongo = new MongoClient(new MongoClientURI("mongodb://192.168.0.132:27018"))
    mongo.getDatabase("test").drop()
  }


  def after = {}

  "TypeHandler" should {

    "convert List[Int]" in {
      val th = implicitly[TypeHandler[List[Int]]]
      val list = List(1, 2, 3)
      val dboList = th.toDBObject(list)
      th.fromDBObject(dboList) must beEqualTo(list)
    }


    "convert List[TestEntity]" in {
      val e = new TestEntity
      e.intF := Some(10)
      e.strF := "test"

      val ce = new TestComplexEntity
      ce.children := List(e, e, e)

      val o = ce.toDBObject
      val ce2 = new TestComplexEntity
      ce2.fromDBObject(o)

      ce2.children.get.length must beEqualTo(3)

      ce2.children.get map {
        ch =>
          ch.intF.get must beEqualTo(10)
          ch.strF.get must beEqualTo("test")
      }
    }


    "convert List[Map[String, Option[Int]]" in {
      val th = implicitly[TypeHandler[List[Map[String, Option[Int]]]]]

      val list = List(
        Map("1" -> Some(1), "2" -> Some(2)),
        Map("10" -> Some(10), "20" -> Some(20))
      )
      val dboList = th.toDBObject(list)
      th.fromDBObject(dboList) must beEqualTo(list)
    }


    "convert List[Map[String, Int]" in {
      val th = implicitly[TypeHandler[List[Map[String, Int]]]]

      val list = List(
        Map("1" -> 1, "2" -> 2),
        Map("10" -> 10, "20" -> 20)
      )
      val dboList = th.toDBObject(list)
      th.fromDBObject(dboList) mustEqual list
    }
  }
}
