package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Detection, IO}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainingSet {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def getTrainingAndTestingDataFrames(paths: Seq[String], fileType: Option[String], limit: Int, verbose: Boolean, spark: SparkSession): Option[DataFrame] = {
    val sources = paths.map(i => IO.loadFile(fileType, i, spark))
    if (sources.nonEmpty) {
      if (sources.size == 1 && sources.head.isDefined) {
        if (verbose) {
          log.debug("training source:\n")
          sources.head.get._1.take(limit).foreach(log.debug)
        }
        Some(sources.head.get._1)
      } else {
        val training = sourceIndexes(sources.filter(s => s.isDefined).map(s => s.get._1), limit)
        if (training.isDefined) {
          if (verbose) {
            log.debug("training source:\n")
            training.get.take(limit).foreach(log.debug)
          }
          Some(training.get)
        } else {
          None
        }
      }
    } else {
      None
    }
  }

  private def builder(f: (DataFrame, Int) => Option[String])(limit: Int)(expr: => List[DataFrame], acc: Map[DataFrame, String]): Map[DataFrame, String] = expr match {
    case Nil => acc
    case w :: tail if f(w, limit).isDefined => builder(f)(limit)(tail, acc + (w -> f(w, limit).get))
  }

  private def sourceIndexes(sources: Seq[DataFrame], limit: Int): Option[DataFrame] = {
    val indexes = builder(Detection.detectIndexColumn)(limit)(sources.toList, Map())
    val texts = builder(Detection.detectTextColumn)(limit)(sources.toList, Map())
    val values = builder(Detection.detectValueColumn)(limit)(sources.toList, Map())
    val categories = builder(Detection.detectCategoricalColumn)(limit)(sources.toList, Map())
    // check if we have data we can work with
    if (texts.size == 1 && ((values.isEmpty && categories.size == 1) || (values.size == 1 && categories.isEmpty))) {
      // if we don't have an amount of indexes equal to the content we have failed automatic detection
      if (indexes.keys.toSet.size == (texts.keys.toList ++ values.keys.toList ++ categories.keys.toList).toSet.size) {
        Some(indexes.reduce((l, r) => (l._1.join(r._1, l._1.col(l._2) <=> r._1.col(r._2)), l._2))._1)
      }
    }
    None
  }

}
