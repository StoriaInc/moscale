package com.codebranch.scala.mongodb

class Field[T](val key: String, default: Option[T] = None)(implicit tm: Manifest[T], th: TypeHandler[Option[T]],
                                                           fm: collection.mutable.HashMap[String, Field[_]]) extends Serializable {

  thisField => fm += key -> thisField

	private var value = default

	def set(v: Option[T]): Unit = {
		value = v
	}

  def set(v: T): Unit = {
    value = Some(v)
  }


	@inline def := (v: Option[T]) = set(v)
  @inline def := (v: T) = set(v)

	def toOption = value

	def toDBObject =
    try {
      th.toDBObject(value)
    } catch {
      case e: UnexpectedType =>
        Logger.error(s"Couldn't convert field `${key}` with value `${value}` to DBObject")
        throw e
    }

	def fromDBObject(dbObject: Object, partial: Boolean = false): this.type = {
		value = th.fromDBObject(dbObject, partial)
		this
	}


  override def equals(obj: Any): Boolean =
    obj match {
      case that: Field[T] => that.value == this.value
      case _ => false
    }

  override def hashCode: Int =
    value.hashCode()

  override def toString: String =
    value.toString
}


object Field {
  import scala.language.implicitConversions

  implicit def field2Option[T](f: Field[T]): Option[T] = f.toOption

  def apply[T](key: String, default: Option[T] = None)(implicit tm: Manifest[T], th: TypeHandler[Option[T]],
      fm: collection.mutable.HashMap[String, Field[_]]): Field[T] =
    new Field[T](key, default)
}

