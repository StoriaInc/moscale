package com.codebranch.scala.mongodb

import scala.language.implicitConversions

abstract class Value
{
	val dbObject : Object

	def asValue[T](implicit th : TypeHandler[T]) : T = th.fromDBObject(dbObject)
}


object Value
{
	implicit def asValue[T](v : T)(implicit th : TypeHandler[T]) : Value =
		new Value {
			val dbObject = th.toDBObject(v)
		}

	implicit def asValue(v : Option[Nothing]) : Value =
		new Value {
			val dbObject = null
		}

  implicit def asValue[T](v : Option[T])(implicit th : TypeHandler[Option[T]]) : Value =
		new Value {
			val dbObject = th.toDBObject(v)
		}

	implicit def asValue[T](v : Field[T]) : Value =
		new Value {
			val dbObject = v.toDBObject
    }

	def apply[T](v : T)(implicit th : TypeHandler[T]) = asValue(v)

	def fromDBObject(o : Object) = new Value {
		val dbObject = o
	}

	type Map = scala.collection.immutable.Map[String, Value]

	object Map {
		def apply(ev : (String, Value)*) = scala.collection.immutable.Map[String, Value](ev:_*)
	}

  type List = scala.collection.immutable.List[Value]

  object List {
    def apply(ev : Value*) = scala.collection.immutable.List[Value](ev:_*)
  }
}
