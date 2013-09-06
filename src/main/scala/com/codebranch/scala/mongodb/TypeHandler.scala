package com.codebranch.scala.mongodb


import com.mongodb.{BasicDBObject, BasicDBList, DBObject}
import org.bson.types.ObjectId
import scala.language.implicitConversions
import java.lang.{
  Integer => JInteger,
  Long => JLong,
  Double => JDouble,
  Boolean => JBoolean}

import collection.{immutable, mutable}
import collection.mutable.ListBuffer
import org.joda.time.{DateTimeZone, DateTime}
import java.util.regex.Pattern
import java.net.URL
import scala.Enumeration



class UnexpectedType(msg : String, cause : Throwable = null) extends RuntimeException(msg, cause)


object UnexpectedType {
	def unexpectedNull = new UnexpectedType("Unexpected null toOption")

	def unexpectedType(cur : String, exp : String) : UnexpectedType =
		new UnexpectedType("Unexpected toOption type '" + cur + "'. Expecting toOption of type '" + exp + "'.")

	def unexpectedType(cur : Class[_], exp : Class[_]) : UnexpectedType =
		unexpectedType(cur.getCanonicalName, exp.getCanonicalName)

	def unexpectedType(cur : Class[_], exp : Manifest[_]) : UnexpectedType =
		unexpectedType(cur.getCanonicalName, exp.toString)
}

import UnexpectedType._



abstract class TypeHandler[T] extends Serializable
{
	def fromDBObject(dbo : Object, partial: Boolean = false) : T
	def toDBObject(v : T) : Object
}


abstract class NotNullTypeHandler[T] extends TypeHandler[T]
{
	def fromDBObject(dbo: Object, partial: Boolean = false) = dbo match {
		case null => throw unexpectedNull
		case _ =>	fromDBObjectNN(dbo, partial) match {
				case null => throw unexpectedNull
				case v => v
			}
	}

	def toDBObject(v: T) = v match {
		case null => throw unexpectedNull
		case _ =>	toDBObjectNN(v)
	}

	def fromDBObjectNN(dbo : Object, partial: Boolean = false) : T

	def toDBObjectNN(v: T) : Object
}



class AsIsTypeHandler[T <: Object](implicit tm : Manifest[T])
		extends NotNullTypeHandler[T]
{
	override def fromDBObjectNN(dbo: Object, partial: Boolean = false) = dbo match {
		case o : T => o
		case x => throw unexpectedType(x.getClass, tm)
	}

	override def toDBObjectNN(v: T) = v
}


class OptionTypeHandler[T](implicit th : TypeHandler[T])
		extends TypeHandler[Option[T]]
{
	override def fromDBObject(dbo: Object, partial: Boolean = false) = dbo match {
		case null => None
		case _ => Some(th.fromDBObject(dbo, partial))
  }

  override def toDBObject(v: Option[T]) = v match {
    case null => null
    case _ => v.map(th.toDBObject).getOrElse(null)
  }
}


class IntTypeHandler
		extends NotNullTypeHandler[Int]
{
	override def fromDBObjectNN(v: Object, partial: Boolean = false) : Int = v match {
		case v : JInteger => v.intValue
		case x => throw UnexpectedType.unexpectedType(x.getClass, classOf[JInteger])
	}

	override def toDBObjectNN(v: Int) : Object = new JInteger(v)
}


class LongTypeHandler
		extends NotNullTypeHandler[Long]
{
	override def fromDBObjectNN(v: Object, partial: Boolean = false) = v match {
		case x : JInteger => x.longValue
		case x : JLong => x.longValue
		case x => throw UnexpectedType.unexpectedType(x.getClass, classOf[JLong])
	}

	override def toDBObjectNN(v: Long) : Object = new JLong(v)
}


class DoubleTypeHandler
		extends NotNullTypeHandler[Double]
{
	override def fromDBObjectNN(v: Object, partial: Boolean) = v match {
		case x : JDouble => x.doubleValue
		case x => throw UnexpectedType.unexpectedType(x.getClass, classOf[Double])
	}

	override def toDBObjectNN(v: Double) = new JDouble(v)
}


class BooleanTypeHandler
		extends NotNullTypeHandler[Boolean]
{
	override def fromDBObjectNN(v: Object, partial: Boolean) = v match {
		case x : JBoolean => x.booleanValue()
		case x => throw UnexpectedType.unexpectedType(x.getClass, classOf[JBoolean])
	}

	override def toDBObjectNN(v: Boolean) = new JBoolean(v)
}


object DateTimeTypeHandler
{
  val Zone = "dateTimeZone"
  val Year = "year"
  val MonthOfYear = "monthOfYear"
  val DayOfMonth = "dayOfMonth"
  val HoursOfDay = "hourOfDay"
  val MinuteOfHour = "minuteOfHour"
  val SecondOfMinute = "secondOfMinute"
  val Milliseconds = "milliseconds"
}


class DateTimeTypeHandler extends NotNullTypeHandler[DateTime] {
  import DateTimeTypeHandler._

	def fromDBObjectNN(dbo: Object, partial: Boolean) : DateTime = dbo match {
		case dbo: DBObject => {
		val millis = dbo.get(Milliseconds) match {
			case m: JLong => m
			case _ =>
				throw new RuntimeException("DBObject should contains milliseconds field")
		}
			val zone = dbo.get(Zone) match {
				case z: String => z
				case _ =>
					throw new RuntimeException("DBObject should contains time zone eld")
			}
			val date = new DateTime().withZone(DateTimeZone.forID(zone))
			date.withMillis(millis)
		}
		case x => throw unexpectedType(x.getClass, classOf[DBObject])
	}

	def toDBObjectNN(v: DateTime) : Object = {
		val dbo = new BasicDBObject()
		dbo.put(Zone, v.getZone.getID)
		dbo.put(Year, v.year.get)
    dbo.put(MonthOfYear, v.monthOfYear.get())
		dbo.put(DayOfMonth, v.dayOfMonth.get())
		dbo.put(HoursOfDay, v.hourOfDay.get())
		dbo.put(MinuteOfHour, v.minuteOfHour.get())
		dbo.put(SecondOfMinute,	v.secondOfMinute.get())
		dbo.put(Milliseconds, v.getMillis)
		dbo
	}
}

class EntityTypeHandler[T <: Entity](implicit m : Manifest[T]) extends NotNullTypeHandler[T] {

  def fromDBObjectNN(v: Object, partial: Boolean = false) = v match {
    case v : DBObject => {
      v.get(Entity.Field.ClassName) match {
        case null =>
          m.runtimeClass.newInstance.asInstanceOf[T].fromDBObject(v, partial)
        case className: String =>
          try {
            Class.forName(className).newInstance().asInstanceOf[T].fromDBObject(v, partial)
          } catch {
            case e: InstantiationException => {
              Logger.error("Could not create instance for className = %s" format className)
              throw e
            }
          }
        case x => throw unexpectedType(x.getClass, classOf[String])
      }
    }
    case x => throw unexpectedType(x.getClass, classOf[DBObject])
  }

	def toDBObjectNN(v: T) = v.toDBObject
}


class ImmutableMapTypeHandlerStringKey[T](implicit th : TypeHandler[T]) extends NotNullTypeHandler[immutable.Map[String, T]] {
	import scala.collection.JavaConversions.mapAsScalaMap

	def fromDBObjectNN (obj: Object, partial: Boolean = false) = obj match {
		case obj: DBObject => {
			mapAsScalaMap(obj.toMap).asInstanceOf[mutable.Map[String, Object]]
				.foldLeft(Map[String, T]()) {
				case (m, (k, ov)) => m + ((k, th.fromDBObject(ov)))
			}
		}
		case x => throw unexpectedType(x.getClass, classOf[DBObject])
	}

	def toDBObjectNN (map: immutable.Map[String, T]) = {
		val o = new BasicDBObject()
		map.foreach {
			case (k, v) => o.put(k, th.toDBObject(v))
		}
		o
	}
}


class ImmutableMapTypeHandlerEnumKey[K <: Enumeration,T](implicit th : TypeHandler[T], mn: Manifest[K])
		extends NotNullTypeHandler[immutable.Map[K#Value, T]] {
	import scala.collection.JavaConversions.{asJavaMap, mapAsScalaMap}

	def fromDBObjectNN (obj: Object, partial: Boolean = false) = obj match {
		case obj: DBObject =>
			mapAsScalaMap(obj.toMap).asInstanceOf[mutable.Map[String, Object]].foldLeft(Map[K#Value, T]()) {
				case (m, (k, ov)) => m + ((EnumTypeHandler.fromString[K](k), th.fromDBObject(ov)))
			}
		case x =>
      throw unexpectedType(x.getClass, classOf[DBObject])
	}

	def toDBObjectNN (map: immutable.Map[K#Value, T]) = {
		val o = new BasicDBObject()
		map.foreach {
			case (k, v) => o.put(k.toString, th.toDBObject(v))
		}
		o
	}
}


object EnumTypeHandler {
	def fromString[T <: Enumeration](s: String)(implicit m: Manifest[T]) =
		m.runtimeClass.getField("MODULE$").get(null).asInstanceOf[T].withName(s)
}


class EnumTypeHandler [T <:Enumeration](implicit m: Manifest[T]) extends NotNullTypeHandler[T#Value] {
	def fromDBObjectNN(dbo: Object, partial: Boolean = false) = dbo match {
		case s:String => 	{
			EnumTypeHandler.fromString(s)
//			m.runtimeClass.getField("MODULE$").get(null).asInstanceOf[T].withName(s)
		}
	}

	def toDBObjectNN(v: T#Value) = v.toString
}

//// TODO: FIX ME I am buggy
//class MutableMapTypeHandler [V] (implicit th : TypeHandler[V])
//  extends NotNullTypeHandler[mutable.Map[String, V]]
//{
//  import scala.collection.JavaConversions.{asJavaMap, asScalaMap}
//  def fromDBType (v: Object) = v match {
//    case v: DBObject => {
//      val mutMap = mutable.Map[String,V]()
//      val m = asScalaMap(v.toMap).asInstanceOf[mutable.Map[String,Object]]
//      m.foreach( t => {
//	      if (null == t._2) throw UnexpectedType.unexpectedNull
//	      mutMap += (t._1 -> th.fromDBValue(t._2))
//      })
//      mutMap
//    }
//    case _ => throw new Exception("Unexpected type") // TODO: throw specific exception
//  }
//
//
//  def toDBType (v: mutable.Map[String, V]) = new BasicDBObject(asJavaMap(v)) // !!! obj must be converted!
//}


class ListTypeHandler[T](implicit th : TypeHandler[T]) extends NotNullTypeHandler[List[T]] {
	def fromDBObjectNN(v: Object, partial: Boolean = false) = v match {
		case v: BasicDBList => {
			val buffer = ListBuffer[T]()
			val it = v.iterator()

			while (it.hasNext) {
				buffer += th.fromDBObject(it.next())
			}
			buffer.toList
		}
		case x => throw unexpectedType(x.getClass, classOf[BasicDBList])
	}

  def toDBObjectNN(v: List[T]) = {
	  val dbList = new BasicDBList
	  v foreach(x => dbList.add(th.toDBObject(x)))
	  dbList
  }
}


class SetTypeHandler[T](implicit th : TypeHandler[T]) extends NotNullTypeHandler[Set[T]] {
	def fromDBObjectNN(v: Object, partial: Boolean = false) = v match {
		case v: BasicDBList => {
			val buffer = ListBuffer[T]()
			val it = v.iterator()

			while (it.hasNext) {
				buffer += th.fromDBObject(it.next())
			}
			buffer.toSet
		}
		case x => throw unexpectedType(x.getClass, classOf[BasicDBList])
	}

	def toDBObjectNN(v: Set[T]) = {
		val dbList = new BasicDBList
		v foreach(x => dbList.add(th.toDBObject(x)))
		dbList
	}
}

//class ReferenceTypeHandler[T <: EntityId](implicit th : TypeHandler[T], m: Manifest[T])
//		extends NotNullTypeHandler[Reference[T]]
//{
//	def fromDBObjectNN(v: Object, partial: Boolean = false) = v match {
//		case v: DBObject => {
//			Reference.fromDBObject(v)
//		}
//		case x => throw unexpectedType(x.getClass, classOf[DBObject])
//	}
//
//
//	def toDBObjectNN(v: Reference[T]) = v.toDBObject
//}


abstract class ConvertingTypeHandler[T, StorableType](implicit storableTH : TypeHandler[StorableType])
  extends NotNullTypeHandler[T]
{
  def convertToStorableType(v : T) : StorableType
  def convertFromStorableType(v : StorableType) : T


	override def fromDBObjectNN(dbo: Object, partial: Boolean = false) : T =
		convertFromStorableType(storableTH.fromDBObject(dbo))

	override def toDBObjectNN(v: T) =
    storableTH.toDBObject(convertToStorableType(v))
}


package object handlers {
	implicit lazy val intTypeHandler = new IntTypeHandler
	implicit lazy val longTypeHandler = new LongTypeHandler
	implicit lazy val doubleTypeHandler = new DoubleTypeHandler
	implicit lazy val booleanTypeHandler = new BooleanTypeHandler

	implicit lazy val objectIdTypeHandler = new AsIsTypeHandler[ObjectId]
  implicit lazy val patternTypeHandler = new AsIsTypeHandler[Pattern]
	implicit lazy val stringTypeHandler = new AsIsTypeHandler[String]
	implicit lazy val intObjTypeHandler = new AsIsTypeHandler[JInteger]
	implicit lazy val longObjTypeHandler = new AsIsTypeHandler[JLong]
	implicit lazy val doubleObjTypeHandler = new AsIsTypeHandler[JDouble]
	implicit lazy val booleanObjTypeHandler = new AsIsTypeHandler[JBoolean]
	implicit lazy val dbObjectTypeHandler = new AsIsTypeHandler[DBObject]
	implicit lazy val dateTimeTYpeHandler = new DateTimeTypeHandler

	implicit def optionTypeHandler[T](implicit th : TypeHandler[T]) = new OptionTypeHandler[T]

	implicit def entityTypeHandler[T <: Entity](implicit m : Manifest[T]) : TypeHandler[T] =
		new EntityTypeHandler[T]

	implicit def listTypeHandler[T](implicit th : TypeHandler[T]) = new ListTypeHandler[T]

	implicit def setTypeHandler[T](implicit th : TypeHandler[T]) = new SetTypeHandler[T]

  implicit def immutableMapTypeHandlerStringKey[T](implicit th : TypeHandler[T]) =
	  new ImmutableMapTypeHandlerStringKey[T]

	implicit def immutableMapTypeHandlerEnumKey[K <: Enumeration, V](implicit th : TypeHandler[V], m: Manifest[K]) =
		new ImmutableMapTypeHandlerEnumKey[K, V]

//	implicit def referenceTypeHandler[T <: Entity with EntityId](implicit m: Manifest[T]) =	new
//					ReferenceTypeHandler[T]

//  implicit def mutableMapTypeHandler[T](implicit th : TypeHandler[T]) : TypeHandler[mutable.Map[String,T]] =
//    new MutableMapTypeHandler[T]

	implicit def enumTypeHandler[T <: Enumeration](implicit m: Manifest[T]): TypeHandler[T#Value] =
		new EnumTypeHandler[T]()


  implicit lazy val urlTypeHandler = new ConvertingTypeHandler[URL, String]
  {
    def convertFromStorableType(v: String) = new URL(v)

    def convertToStorableType(v: URL) = v.toString
  }

  // other implicits should be moved to separate package object
  implicit def field2Option[T](f: Field[T]): Option[T] = f.toOption
}
