package com.codebranch.scala.mongodb


import org.specs2.mutable.{BeforeAfter, Specification}
import com.mongodb.MongoClientURI
import handlers._


/**
 * User: alexey
 * Date: 5/31/13
 * Time: 5:17 PM
 */
class TypeHandlerTest extends Specification {

  "TypeHandler" should {

    "convert Seq[Int]" in {
      val th = implicitly[TypeHandler[Seq[Int]]]
      val seq = Seq(1, 2, 3)
      val dboList = th.toDBObject(seq)
      th.fromDBObject(dboList) must beEqualTo(seq)
    }

    "convert Set[Int]" in {
      val th = implicitly[TypeHandler[Set[Int]]]
      val set = Set(1, 2, 3)
      val dboList = th.toDBObject(set)
      th.fromDBObject(dboList) must beEqualTo(set)

    }

    "convert List[Int]" in {
      val th = implicitly[TypeHandler[List[Int]]]
      val list = List(1, 2, 3)
      val dboList = th.toDBObject(list)
      th.fromDBObject(dboList) must beEqualTo(list)
    }

    "convert Seq[List[Int]]" in {
      val th = implicitly[TypeHandler[Seq[List[Int]]]]
      val list = Seq(List(1, 2, 3), List(2))
      val dboList = th.toDBObject(list)
      th.fromDBObject(dboList) must beEqualTo(list)
    }


    "convert Option[List[Entity]]" in {
      val th = implicitly[TypeHandler[Option[List[Entity]]]]
      val list = Some(List(new TestEntity, new TestEntity))
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
