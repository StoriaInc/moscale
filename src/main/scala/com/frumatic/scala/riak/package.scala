package com.frumatic.scala

package object riak {
  import scala.language.implicitConversions

  implicit def boolean2SetBucketBooleanOption(value: Boolean): BooleanOption = BooleanOption(Some(value))
  implicit def boolean2SetBucketIntOption(value: Int): IntOption = IntOption(Some(value))
}
