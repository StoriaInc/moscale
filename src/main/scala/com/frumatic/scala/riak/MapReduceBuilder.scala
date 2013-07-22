package com.frumatic.scala.riak

class MapReduceBuilder[T <: RiakEntity] {

  var bucketName: String = _
  var indexName: Option[String] = None

  var keyFilters: Option[Seq[KeyFilter]] = None
  var rangeFilter: Option[(String, String)] = None
  var listFilter: Option[Seq[String]] = None
  var phases: List[String] = Nil

  def bucketKeyInputs(bucketName: String): MapReduceBuilder[T] = {
    this.bucketName = bucketName
    this
  }

  def indexKeyInputs(bucketName: String, indexName: String): MapReduceBuilder[T] = {
    this.bucketName = bucketName
    this.indexName = Option(indexName)
    this
  }

  def filter(filters: KeyFilter*): MapReduceBuilder[T] = {
    this.keyFilters = Some(filters)
    this
  }

  def filterRange(start: String, end: String): MapReduceBuilder[T] = {
    this.rangeFilter = Some(start -> end)
    this
  }

  def filterList(values: String*): MapReduceBuilder[T] = {
    this.listFilter = Some(values)
    this
  }

  def withMapPhase[K <: RiakEntity](map: String): MapReduceBuilder[K] = {
    val mrb = new MapReduceBuilder[K]
    mrb.bucketName = bucketName
    mrb.indexName = indexName
    mrb.keyFilters = keyFilters
    mrb.rangeFilter = rangeFilter
    mrb.listFilter = listFilter
    mrb.phases = map :: phases
    mrb
  }

  def build: String = {
    require(bucketName != null, "Bucket name must be specified")
    require(!(indexName.isDefined && (keyFilters.isDefined || listFilter.isDefined)), "Can't use 2i with key filters")
    require(!(keyFilters.isDefined && rangeFilter.isDefined ||
      keyFilters.isDefined && listFilter.isDefined ||
      rangeFilter.isDefined && listFilter.isDefined), "Only one type of key filters must be specified")
    val inputsStr =
      if (listFilter.isDefined) {
        val inputTemplate = "[\"" + bucketName + "\", \"%s\"]"
        "\"inputs\": [" + listFilter.get.map(key => inputTemplate format key).mkString(", ") + "]"
      } else if (keyFilters.isDefined || rangeFilter.isDefined) {
        val indexSelector =
          indexName match {
            case Some(name) =>
              "\"index\": \"" + name + "\","
            case None =>
              ""
          }
        val filterStr =
          if (keyFilters.isDefined)
            "\"key_filters\": [" + keyFilters.get.map(_.toString).mkString("], [") + "]"
          else if (rangeFilter.isDefined)
            "\"start\": \"" + rangeFilter.get._1 + "\", \"end\": \"" + rangeFilter.get._2 + "\""
        s"""
        |  "inputs": {
        |    "bucket": "$bucketName",
        |    $indexSelector
        |    $filterStr
        |  }
        """.trim.stripMargin
        /**/
      } else {
        "\"inputs\": \"" + bucketName + "\""
      }

    val phases = this.phases.reverse.map(phase => "{" + phase + "}").mkString(", ")

    val query = s"""{
      |  $inputsStr,
      |  "query": [$phases]
      |}
    """.trim.stripMargin.replaceAll("\n", "").replaceAll("[ ]+", " ")
    /**/
    Logger.debug(s"MAP-REDUCE $query")
    query
  }
}

sealed trait KeyFilter

object IntToString extends KeyFilter { override def toString = """["int_to_string"]""" }
object StringToInt extends KeyFilter { override def toString = """["string_to_int"]""" }
object FloatToString extends KeyFilter { override def toString = """["float_to_string"]""" }
object StringToFloat extends KeyFilter { override def toString = """["string_to_float"]""" }
object ToUpper extends KeyFilter { override def toString = """["to_upper"]""" }
object ToLower extends KeyFilter { override def toString = """["to_lower"]""" }
case class Tokenize(delim: String, pos: Int) extends KeyFilter { override def toString = s"""["tokenize", "$delim", $pos]""" }
object UrlDecode extends KeyFilter { override def toString = """["url_decode"]""" }
case class GreaterThan(value: Int) extends KeyFilter { override def toString = s"""["greater_than", $value]""" }
case class GreaterThanEq(value: Int) extends KeyFilter { override def toString = s"""["greater_than_eq", $value]""" }
case class LessThan(value: Int) extends KeyFilter { override def toString = s"""["less_than", $value]""" }
case class LessThanEq(value: Int) extends KeyFilter { override def toString = s"""["less_than_eq", $value]""" }
case class Between(lower: Int, upper: Int) extends KeyFilter { override def toString = s"""["between", $lower, $upper]""" }
case class Matches(regex: String) extends KeyFilter { override def toString = s"""["matches", "$regex"]""" }
case class Eq(value: String) extends KeyFilter { override def toString = s"""["eq", "$value"]""" }
case class Neq(value: String) extends KeyFilter { override def toString = s"""["neq", "$value"]""" }
case class SetMember(values: String*) extends KeyFilter { override def toString = s"""["set_member", "${values.mkString("\", \"")}"]""" }
case class StartsWith(value: String) extends KeyFilter { override def toString = s"""["starts_with", "$value"]""" }
case class EndsWith(value: String) extends KeyFilter { override def toString = s"""["ends_with", "$value"]""" }

case class Not(filter: KeyFilter) extends KeyFilter {
  override def toString = {
    val f = filter.toString
    s"""["not", [$f]"""
  }
}
case class And(filters: KeyFilter*) extends KeyFilter {
  override def toString = {
    val f = filters.map(_.toString).mkString("], [")
    s"""["and", [$f]"""
  }
}
case class Or(filters: KeyFilter*) extends KeyFilter {
  override def toString = {
    val f = filters.map(_.toString).mkString("], [")
    s"""["or", [$f]"""
  }
}

object CommonPhase {
  val GetValues = "\"map\": {\"language\": \"javascript\",\"name\": \"Riak.mapValues\"}"
  val WalkLinks = "\"link\":{}"
}