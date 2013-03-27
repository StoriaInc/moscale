package com.codebranch.scala.mongodb

import org.slf4j.LoggerFactory

object Logger {
  private val logger = LoggerFactory.getLogger("mongo")

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
