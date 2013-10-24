package com.codebranch.scala.mongodb

import com.{mongodb => jmdb}
import handlers._
import com.mongodb.DBObject

/**
 * User: alexey
 * Date: 10/1/12
 * Time: 3:12 PM
 */
class Cursor[T](val jmdbCursor: jmdb.DBCursor)(implicit th: TypeHandler[T]) {

  private def stream(c: jmdb.DBCursor): Stream[T] = {
    if (c.hasNext)
      th.fromDBObject(c.next()) #:: stream(c)
    else Stream.empty
  }

  def getStream = stream(jmdbCursor.copy())

  def getIterator = stream(jmdbCursor.copy()).toIterator

  def copy : Cursor[T] = new Cursor[T](jmdbCursor.copy)

  def sort(sorters: DBObject*) : Cursor[T] = new Cursor[T](jmdbCursor.copy.sort(DBObjectGen.composeExprs(sorters.head, sorters.tail: _*)))

  def skip(n : Int) : Cursor[T] = new Cursor[T](jmdbCursor.copy.skip(n))

  def limit(n : Int) : Cursor[T] = new Cursor[T](jmdbCursor.copy.limit(n))

  def count: Int = jmdbCursor.count()
}

class RawCursor(val jmdbCursor: jmdb.DBCursor) {

  private def stream(c: jmdb.DBCursor): Stream[DBObject] = {
    if (c.hasNext)
      c.next() #:: stream(c)
    else Stream.empty
  }

  def getStream = stream(jmdbCursor.copy())

  def getIterator = stream(jmdbCursor.copy()).toIterator

  def copy: RawCursor = new RawCursor(jmdbCursor.copy)

  def sort(sorters: DBObject*): RawCursor = new RawCursor(jmdbCursor.copy.sort(DBObjectGen.composeExprs(sorters.head, sorters.tail: _*)))

  def skip(n: Int): RawCursor = new RawCursor(jmdbCursor.copy.skip(n))

  def limit(n: Int): RawCursor = new RawCursor(jmdbCursor.copy.limit(n))

  def count: Int = jmdbCursor.count()
}
