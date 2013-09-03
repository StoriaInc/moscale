package com.codebranch.scala.mongodb

class Field[T](val key : String, default : Option[T] = None)(implicit tm: Manifest[T], th: TypeHandler[Option[T]],
                                                             fm: collection.mutable.HashMap[String, Field[_]]) extends Serializable {

  thisField => fm += key -> thisField

	private var _value = default

	def set(v : Option[T]) {
		_value = v
	}


	def := (v : Option[T]) = set(v)
  def := (v : T) = set(Some(v))


	def toOption = _value


	def toDBObject = try {
		th.toDBObject(_value)
	} catch {
		case e: UnexpectedType => {
			Logger.error(s"Couldn't convert field `${key}` with value `${_value}" +
			             "` to DBObject")
			//TODO: throw specific exception
			throw new UnexpectedType(s"Couldn't convert field `${key}` with value `${_value}" +
			             "` to DBObject")
		}
	}


	def fromDBObject(dbObject : Object, partial: Boolean = false) : this.type = {
		_value = th.fromDBObject(dbObject, partial)
		this
	}


  override def equals(obj: Any): Boolean = obj match {
    case that: Field[T] => that._value == this._value
    case _ => false
  }

  override def hashCode(): Int = _value.hashCode()

  override def toString = _value.toString
}


object Field {
  import scala.language.implicitConversions

  implicit def field2Option[T](f: Field[T]): Option[T] = f.toOption

  def apply[T](key: String, default: Option[T] = None)(implicit tm: Manifest[T], th: TypeHandler[Option[T]],
      fm: collection.mutable.HashMap[String, Field[_]]): Field[T] =
    new Field[T](key, default)
}

