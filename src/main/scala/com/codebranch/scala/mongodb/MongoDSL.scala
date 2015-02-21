package com.codebranch.scala.mongodb

import com.mongodb.{BasicDBList, BasicDBObject}
import java.lang.{Integer => JInteger, Boolean => JBoolean, Long => JLong}
import Query.Expression
import java.util.Date

class DBObjectGen(key: String) {

  import DBObjectGen._
  import MongoDSL.$array

  def $eq[T](value: T)(implicit th: TypeHandler[T]): Expression = compose(key, th.toDBObject(value))

  def $eq[T](value: Field[T])(implicit th: TypeHandler[T]): Expression = compose(key, th.toDBObject(value.get))

  @inline def --[T](value: T)(implicit th: TypeHandler[T]): Expression = $eq(value)

  @inline def --[T](value: Field[T])(implicit th: TypeHandler[T]): Expression = $eq(value)

  def $ne[T](value: T)(implicit th: TypeHandler[T]): Expression = compose(key, compose("$ne", th.toDBObject(value)))

  def $ne[T](value: Field[T])(implicit th: TypeHandler[T]): Expression = compose(key, compose("$ne", th.toDBObject(value.get)))


  def $gt(value: JLong): Expression = compose(key, compose("$gt", value))

  @inline def >(value: Long): Expression = $gt(value)

  def $gte(value: JLong): Expression = compose(key, compose("$gte", value))

  @inline def >=(value: Long): Expression = $gte(value)

  def $lt(value: JLong): Expression = compose(key, compose("$lt", value))

  @inline def <(value: Long): Expression = $lt(value)

  def $lte(value: JLong): Expression = compose(key, compose("$lte", value))

  @inline def <=(value: Long): Expression = $lte(value)



  def $gt(value: Date): Expression = compose(key, compose("$gt", value))

  @inline def >(value: Date): Expression = $gt(value)

  def $gte(value: Date): Expression = compose(key, compose("$gte", value))

  @inline def >=(value: Date): Expression = $gte(value)

  def $lt(value: Date): Expression = compose(key, compose("$lt", value))

  @inline def <(value: Date): Expression = $lt(value)

  def $lte(value: Date): Expression = compose(key, compose("$lte", value))

  @inline def <=(value: Date): Expression = $lte(value)



  def $size[T](value: T)(implicit th: TypeHandler[T]): Expression = compose(key, compose("$size", th.toDBObject(value)))

  def $mod(divisor: JInteger, remainder: JInteger): Expression = compose(key, compose("$mod", {
    val list = new BasicDBList
    list.add(divisor)
    list.add(remainder)
    list
  }))

  def $regex(regex: String, options: String = ""): Expression = {
    val regexObj = new BasicDBObject("$regex", regex)
    if (options.length > 0)
      regexObj.put("$options", options)
    compose(key, regexObj)
  }

  def $exists(value: JBoolean): Expression = compose(key, compose("$exists", value))

  @inline def isDefined: Expression = $exists(true)

  @inline def isEmpty: Expression = $exists(false)

//  def $in[T](values: T*)(implicit th: TypeHandler[T]): Expression = groupOp("$in", values: _*)
  def $in[T](values: Seq[T])(implicit th: TypeHandler[T]): Expression = groupOp("$in", values)
//  def $in[T](values: Field[T]*)(implicit th: TypeHandler[T]): Expression = groupOp("$in", values: _*)
//  def $in[T](values: Seq[Field[T]])(implicit th: TypeHandler[T], m: Manifest[Field[T]]): Expression = groupOp("$in", values)

//  def $nin[T](values: T*)(implicit th: TypeHandler[T]): Expression = groupOp("$nin", values: _*)
  def $nin[T](values: Seq[T])(implicit th: TypeHandler[T]): Expression = groupOp("$nin", values)
//  def $nin[T](values: Field[T]*)(implicit th: TypeHandler[T]): Expression = groupOp("$nin", values: _*)
//  def $nin[T](values: Seq[Field[T]])(implicit th: TypeHandler[T], m: Manifest[Field[T]]): Expression = groupOp("$nin", values)

//  def $all[T](values: T*)(implicit th: TypeHandler[T]): Expression = groupOp("$all", values: _*)
  def $all[T](values: Seq[T])(implicit th: TypeHandler[T]): Expression = groupOp("$all", values)
//  def $all[T](values: Field[T]*)(implicit th: TypeHandler[T]): Expression = groupOp("$all", values: _*)
//  def $all[T](values: Seq[Field[T]])(implicit th: TypeHandler[T], m: Manifest[Field[T]]): Expression = groupOp("$all", values)

//  @inline private def groupOp[T](name: String, values: T*)(implicit th: TypeHandler[T]): Expression =
//    compose(key, compose(name, $array(values: _*)))

  @inline private def groupOp[T](name: String, values: Seq[T])(implicit th: TypeHandler[T]): Expression =
    compose(key, compose(name, $array(values)))

//  @inline private def groupOp[T](name: String, values: Field[T]*)(implicit th: TypeHandler[T]): Expression =
//    groupOp(name, values.map(_.get): _*)
//
//  @inline private def groupOp[T](name: String, values: Seq[Field[T]])(implicit th: TypeHandler[T]): Expression =
//    groupOp(name, values.map(_.get))

  def $elemMatch(expr: Expression): Expression = compose(key, compose("$elemMatch", expr))

}

object DBObjectGen {

  def compose(key: String, expr: Object): Expression =
    new BasicDBObject(key, expr)

  def composeExprs(expr: Expression, exprs: Expression*): Expression = {
    import scala.collection.JavaConversions.asScalaSet
    val result = new BasicDBObject()
    for (key <- expr.keySet()) {
      result.append(key, expr.get(key))
    }
    for (expr <- exprs; key <- expr.keySet()) {
      result.append(key, expr.get(key))
    }
    result
  }

}

class ExprGen(expr: Expression) {

  import MongoDSL._

  def &&(other: Expression): Expression =
    $and(expr, other)

  def ||(other: Expression): Expression =
    $or(expr, other)

  def unary_! : Expression =
    $not(expr)
}

object MongoDSL {

  import DBObjectGen._
  import scala.language.implicitConversions
  import handlers._

  val EmptyExpression: Expression = null

  implicit def String2DBObjectGen(s: String) =
    new DBObjectGen(s)

  implicit def Expression2ExprGen(expr: Expression) =
    new ExprGen(expr)

  def $array[T](values: Seq[T])(implicit th: TypeHandler[T]): Expression = {
    val list = new BasicDBList
    values.filter(_ != null).foreach(e => list.add(th.toDBObject(e)))
    list
  }

  def $array(expr: Expression, exprs: Expression*): Expression = {
    if (exprs.isEmpty)
      expr
    else {
      val list = new BasicDBList
      if (expr != null) list.add(expr)
      exprs.filter(_ != null).foreach(e => list.add(e))
      list
    }
  }

  private def $objArray(objs: Object*): Expression = {
    val list = new BasicDBList
    objs.filter(_ != null).foreach(list.add)
    list
  }

  def $value[T](value: T)(implicit th: TypeHandler[T]): Expression = {
    th.toDBObject(value).asInstanceOf[Expression]
  }

  private def isContainer(contType: String, expr: Expression): Boolean =
    expr match {
      case bdbo: BasicDBObject if bdbo.keySet.size == 1 && bdbo.containsField(contType) => true
      case _ => false
    }

  def $and(expr: Expression, exprs: Expression*): Expression = {
    if (expr == EmptyExpression) {
      $and(exprs.head, exprs.tail:_*)
    } else {
      exprs.filter(_ != EmptyExpression).foreach(expr.putAll)
      expr
    }
  }

  def $or(expr: Expression, exprs: Expression*): Expression =
    if (exprs.isEmpty)
      expr
    else if (isContainer("$or", expr)) {
      val list = expr.get("$or").asInstanceOf[BasicDBList]
      exprs.foreach(list.add)
      expr
    } else
      compose("$or", $array(expr, exprs: _*))

  def $each[T](values: Seq[T])(implicit th: TypeHandler[T]): Expression =
    compose("$each", $array(values))

  def $nor(expr: Expression, exprs: Expression*): Expression =
    compose("$nor", $array(expr, exprs: _*))

  def $not(expr: Expression): Expression =
    compose("$not", expr)

  //=====================================================================================
  // Update-related
  //=====================================================================================

  def $inc(expr: Expression, exprs: Expression*): Expression =
    compose("$inc", composeExprs(expr, exprs: _*))

  def $set(expr: Expression, exprs: Expression*): Expression =
    compose("$set", composeExprs(expr, exprs: _*))

  def $setOnInsert(expr: Expression, exprs: Expression*): Expression =
    compose("$setOnInsert", composeExprs(expr, exprs: _*))

  def $rename(expr: Expression, exprs: Expression*): Expression =
    compose("$rename", composeExprs(expr, exprs: _*))

  def $unset(expr: Expression, exprs: Expression*): Expression =
    compose("$unset", composeExprs(expr, exprs: _*))

  def $addToSet(expr: Expression, exprs: Expression*): Expression =
    compose("$addToSet", composeExprs(expr, exprs: _*))

  def $push(expr: Expression, exprs: Expression*): Expression =
    compose("$push", composeExprs(expr, exprs: _*))

  def $pushAll(expr: Expression): Expression =
    compose("$pushAll", expr)

  def $pull(expr: Expression, exprs: Expression*): Expression =
    compose("$pull", composeExprs(expr, exprs: _*))

  def $pullAll(expr: Expression, exprs: Expression*): Expression =
    compose("$pullAll", composeExprs(expr, exprs: _*))


  //=====================================================================================
  // Aggregation-related
  //=====================================================================================


  def $project(expr: Expression, exprs: Expression*): Expression =
    compose("$project", composeExprs(expr, exprs: _*))

  def $match(expr: Expression, exprs: Expression*): Expression =
    compose("$match", composeExprs(expr, exprs: _*))

  def $limit(value: JLong): Expression =
    compose("$limit", value)

  def $skip(value: JLong): Expression =
    compose("$skip", value)

  def $unwind(expr: Expression, exprs: Expression*): Expression =
    compose("$unwind", composeExprs(expr, exprs: _*))

  def $group(expr: Expression, exprs: Expression*): Expression =
    compose("$group", composeExprs(expr, exprs: _*))

  def $sort(expr: Expression, exprs: Expression*): Expression =
    compose("$sort", composeExprs(expr, exprs: _*))

  def $sum[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$sum", $array[T](values))

  def $add[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$add", $array[T](values))

  def $add[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$add", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $eq[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$eq", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $neq[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$neq", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $gt[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$gt", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $gte[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$gte", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $lt[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$lt", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $lte[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$lte", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $multiply[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$multiply", $array[T](values))

  def $multiply[T1, T2](value1: T1, value2: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$multiply", $objArray(th1.toDBObject(value1), th2.toDBObject(value2)))

  def $min[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$min", $array[T](values))

  def $max[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$max", $array[T](values))

  def $avg[T](values: T*)(implicit th: TypeHandler[T]): Expression =
    compose("$avg", $array[T](values))

  def $cond[T1, T2](`if`: Expression, `then`: T1, `else`: T2)(implicit th1: TypeHandler[T1], th2: TypeHandler[T2]): Expression =
    compose("$cond", $objArray(`if`, th1.toDBObject(`then`), th2.toDBObject(`else`)))

  def $cond(`if`: Expression, `then`: Expression, `else`: Expression): Expression =
    compose("$cond", $objArray(`if`, `then`, `else`))

}
