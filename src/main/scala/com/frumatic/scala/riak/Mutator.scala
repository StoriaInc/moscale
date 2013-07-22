package com.frumatic.scala.riak

trait Mutator[T <: RiakEntity] {
  def mutate(oldValue: T, newValue: T): T
}

class OverwriteMutator[T <: RiakEntity] extends Mutator[T] {

  def mutate(oldValue: T, newValue: T): T = newValue

}
