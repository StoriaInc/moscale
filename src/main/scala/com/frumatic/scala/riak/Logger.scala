package com.frumatic.scala.riak

import org.slf4j.LoggerFactory

object Logger {
  private val logger = LoggerFactory.getLogger("scala-riak")

  def debug (message : => String) {
    logger.debug(message)
  }

  def error (message : => String) {
    logger.error(message)
  }

  def error (message : => String, error: => Throwable) {
    logger.debug(message, error)
  }
}
