package com.codebranch.scala.mongodb


import com.mongodb.DBObject
import com.sun.istack.internal.NotNull



class Field[T](val key : String, default : Option[T] = None)
              (implicit val tm : Manifest[T],
               th : TypeHandler[Option[T]],
               val entityMetadata : EntityMetadata#Metadata)
{
	thisField =>

	entityMetadata.fieldsMap = entityMetadata.fieldsMap + ((key, thisField))

	private var _value = default


	def set(v : Option[T]) {
		_value = v
	}


	def := (v : Option[T]) = set(v)


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


  override def hashCode(): Int = super.hashCode()


  override def toString = _value.toString
}


object Field
{
	def apply[T](key : String, default : Option[T] = null)(
		implicit m : Manifest[T],
		th : TypeHandler[Option[T]],
		entityMetadata : EntityMetadata#Metadata)
	= new Field[T](key, default)
}

//object OptionalField
//{
//	def apply[T](key : String, default : Option[T] = None)(
//		implicit m : Manifest[Option[T]],
//		th : TypeHandler[Option[T]],
//		entityMetadata : EntityMetadata#Metadata)
//	= new Field[Option[T]](key, default)
//
//	def apply[T](key : String, default : T)(
//		implicit m : Manifest[Option[T]],
//		th : TypeHandler[Option[T]],
//		entityMetadata : EntityMetadata#Metadata)
//	= new Field[Option[T]](key, Some(default))
//}
