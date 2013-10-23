package com.codebranch.scala.mongodb

import com.mongodb.{DBCollection => JDBCollection, DBObject, DBCursor}
import Query.Expression

object Query {
  type Expression = DBObject
}
