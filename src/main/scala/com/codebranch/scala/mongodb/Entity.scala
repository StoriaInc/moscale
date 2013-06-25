package com.codebranch.scala.mongodb


import com.mongodb.{BasicDBObject, DBObject}
import collection.immutable.HashMap
import handlers._
import org.bson.types.ObjectId


trait EntityMetadata extends Serializable
{
  class Metadata extends Serializable {
    var fieldsMap = new HashMap[String, Field[_]]
  }

  implicit lazy val entityMetadata : Metadata = new Metadata
}


trait EntityId extends EntityMetadata
{
  import EntityId.Field.Id
  val id = Field(Id, Some(new ObjectId()))
}


object EntityId {
  object Field {
    val Id = "_id"
  }
}

trait FieldValidator extends EntityMetadata {
  def isValid = validate.isEmpty
  def validate: Map[String, Seq[String]] = entityMetadata.fieldsMap map {
    case (k, f) =>
       (k -> f.validate)
  } filterNot {
    case (k, e) =>
      e.isEmpty
  }
}


class Entity extends EntityMetadata with Cloneable with Serializable
{
  import Entity.Field._

  val className = Field[String](ClassName, Some(this.getClass.getName))

	def toDBObject : DBObject =
	{
		val dbObject = new BasicDBObject
		entityMetadata.fieldsMap foreach {
			case (k, v) =>
				dbObject.put(v.key, v.toDBObject)
		}
		dbObject
	}

	def fromDBObject(dbObject : DBObject, partial: Boolean = false) : this.type =
	{
    entityMetadata.fieldsMap foreach {
      case (k, v) => try {
        if (dbObject.containsField(v.key) || !partial)
          v.fromDBObject(dbObject.get(v.key), partial)
          //TODO: partially convertions of inner objects
//        v.fromDBObject(dbObject.get(v.key))
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


	def merge[T <: Entity](entity : T) {
		fromDBObject(entity.toDBObject, partial = true)
		//TODO: What we should do if T is different class?
		this.className := Some(this.getClass.getName)
	}


	override def clone : this.type = {
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

  def apply(dbo : DBObject, partial: Boolean = false) : Entity =
    dbo.get(Field.ClassName) match {
      case className : String => {
        try {
          Class.forName(className).newInstance().asInstanceOf[Entity].fromDBObject(dbo, partial)
        } catch {
          case e: InstantiationException => {
            Logger.error("Could not create instance for className = %s" format className)
            throw e
          }
        }
      }
      case null => throw new Exception("Unable to reconstruct Entity: className = null")
      case _ => throw new Exception("Unable to reconstruct Entity: className contains unexpected data")
    }
}
