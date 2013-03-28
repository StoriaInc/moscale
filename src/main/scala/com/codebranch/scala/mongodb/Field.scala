package com.codebranch.scala.mongodb


import com.mongodb.DBObject
import com.sun.istack.internal.NotNull



class Field[T](val key : String,
               default : Option[T] = None,
               validator: Seq[Option[T] => Option[String]] = Seq())
              (implicit val tm : Manifest[T],
               th : TypeHandler[Option[T]],
               val entityMetadata : EntityMetadata#Metadata)
{
	thisField =>

	entityMetadata.fieldsMap = entityMetadata.fieldsMap + ((key, thisField))

	private var _value = default

  def validate: Seq[String] = validator flatMap (v => v(_value))

  def isValid: Boolean = validate.isEmpty

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


	//TODO: Why not _value.hashCode()?
  override def hashCode(): Int = super.hashCode()


  override def toString = _value.toString
}


object Field
{
  def apply[T](key : String,
               default : Option[T] = None,
               validator: Seq[Option[T] => Option[String]] = Seq())
              (implicit m : Manifest[T],
               th : TypeHandler[Option[T]],
               entityMetadata : EntityMetadata#Metadata)
  = new Field[T](key, default, validator)
}

