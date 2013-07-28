package com.codebranch.scala.mongodb

import com.mongodb.{DBCollection => JDBCollection, DBObject, DBCursor}
import Query.Expression

object Query {
  type Expression = DBObject

  object Special extends Enumeration {
    val ReturnKeys = Value

    /**
     * You cannot use $snapshot with sharded collections.
     * Do not use $snapshot with $hint or $orderby (or the corresponding cursor.hint() and cursor.sort() methods.)
     */
    val OrderBy = Value
  }

}

abstract class Query[T](implicit th: TypeHandler[T]) {

  protected def eval: DBCursor

  def stream: Stream[T] =
    stream(eval)

  def list: List[T] =
    stream.toList

  def length: Int =
    list.length

  private def stream(c: DBCursor): Stream[T] = {
    if (c.hasNext)
      th.fromDBObject(c.next()) #:: stream(c)
    else
      Stream.empty
  }

}

class FindQuery[T <: Entity](jColl: JDBCollection, query: Expression = null,
                             limit: Option[Int] = None, offset: Option[Int] = None,
                             order: List[Expression] = Nil)
                            (implicit th: TypeHandler[T]) extends Query[T] {

  protected lazy val eval: DBCursor = {
    val cursor = if (query == null) jColl.find() else jColl.find(query)
    limit.foreach(cursor.limit(_))
    offset.foreach(cursor.skip(_))
    Logger.debug(s"FIND $query, $limit, $offset")
    cursor
  }

  def paginate(limit: Int, offset: Int): FindQuery[T] =
    new FindQuery[T](jColl, query, Some(limit), Some(offset))

  private def update(expr: Expression, upsert: Boolean, multi: Boolean): UpdateQuery[T] = {
    require(limit.isEmpty)
    require(offset.isEmpty)
    require(order.isEmpty)
    new UpdateQuery[T](jColl, query, expr, upsert, multi)
  }

  def andUpdate(expr: Expression, exprs: Expression*): UpdateQuery[T] =
    update(MongoDSL.$array(expr, exprs: _*), false, false)

  def andUpsert(expr: Expression): UpdateQuery[T] =
    update(expr, true, false)

  def andUpdateMulti(expr: Expression): UpdateQuery[T] =
    update(expr, false, true)

}

class UpdateQuery[T <: Entity](jColl: JDBCollection, query: Expression = null, update: Expression,
                               upsert: Boolean = false, multi: Boolean = false)
                              (implicit th: TypeHandler[T]) {

  def eval = {
    Logger.debug(s"UPDATE $query, $update, $upsert, $multi")
    jColl.update(query, update, upsert, multi)
  }

}
