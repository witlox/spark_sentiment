package ch.uzh.sentiment.utils

import org.apache.log4j.{LogManager, Logger}

class Timing {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def time[R](tag: String, block: => R): R = {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      log.info(tag + ", elapsed time: " + (t1 - t0) / 1000 + "ms")
      result
  }
}