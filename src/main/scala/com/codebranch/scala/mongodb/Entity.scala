package com.codebranch.scala.mongodb

import com.mongodb.{BasicDBObject, DBObject}
import handlers._
import org.bson.types.ObjectId
import org.bson.BSONObject

trait EntityId { this: Entity =>
  import EntityId.Field.Id
  val id = Field(Id, Some(new ObjectId))(implicitly[Manifest[ObjectId]], implicitly[TypeHandler[Option[ObjectId]]], this.fieldsMap)
}


object EntityId {
  object Field {
    val Id = "_id"
  }
}


trait EntityValidator { this: Entity =>
  private val validators = new collection.mutable.HashMap[String, () => Boolean]
  protected def validator(name: String)(validator: => Boolean): Unit =
    validators += name -> {() => validator}

  def validate: List[String] =
    validators.flatMap { case (k,v) =>
      val result = v()
      if (result) None else Some(k)
    }.toList
}

class Entity extends Cloneable with Serializable {

  implicit val fieldsMap = new collection.mutable.HashMap[String, Field[_]]

	def toDBObject : DBObject = {
		val dbObject = new BasicDBObject
		fieldsMap foreach {
			case (k, v) =>
				dbObject.put(v.key, v.toDBObject)
		}
    dbObject.put(Entity.Field.ClassName, getClass.getCanonicalName)
		dbObject
	}

	def fromDBObject(dbObject : BSONObject, partial: Boolean = false): this.type = {
    fieldsMap foreach {
      case (k, v) => try {
        if (dbObject.containsField(v.key) || !partial)
          v.fromDBObject(dbObject.get(v.key), partial)
          //TODO: partial conversion of inner objects
        else
          Logger.debug(s"Skipping conversion of `${v.key}` field")
      } catch {
        case e: UnexpectedType => {
          Logger.error("Unexpected type error in `%s` field in `%s`"
                                   format (k, this.getClass.getName), e)

          throw new UnexpectedType("Unexpected type in `%s` field in `%s`"
                                   format (k, this.getClass.getName), e)
        }
      }
    }
    this
  }

	def merge[T <: Entity](entity: T): this.type = {
    entity.fieldsMap.foreach { case (key, value) => {
      if (value.isDefined && fieldsMap.contains(key))
        fieldsMap(key).fromDBObject(value.toDBObject)
    }}
    this
  }

	override def clone: this.type = {
		val e = getClass.newInstance().asInstanceOf[this.type]
		e.merge(this)
		e
	}

	def toJsonString = toDBObject.toString

  //TODO: fix toString method with null values
	override def toString = {
    try {
      toDBObject.toString
    } catch {
      case e: UnexpectedType => this.getClass.getSimpleName +
        " with illegal types in fields"
    }
  }
}

object Entity {
  object Field {
    val ClassName = "className"
  }
}
