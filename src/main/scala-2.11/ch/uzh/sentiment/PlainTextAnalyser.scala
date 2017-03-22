package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Stemmer, WordList}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class PlainTextAnalyser(wordList: Broadcast[WordList]) extends Serializable {

  lazy val wl: WordList = wordList.value

  private def getValue(words: Array[String]): Option[Double] = {
    if (words.count(w => wl.value(w) != 0.0) != 0) {
      Some(words.map(w => wl.value(w)).sum / words.count(w => wl.value(w) != 0.0))
    } else {
      None
    }
  }

  private def computeSentiment(text: String): Double = {
    val words = text.split(" ")
    val raw = getValue(words)
    val stem = getValue(words.map(w => Stemmer.stem(w)))
    if (raw.isDefined) {
      if (stem.isDefined) {
        (raw.get + stem.get) / 2
      } else {
        raw.get
      }
    } else if (stem.isDefined) {
      stem.get
    } else {
      0.0
    }
  }

  private def computeSentimentWrapper: (String => Double) = (text: String) => computeSentiment(text)

  def computeSentimentUDF: UserDefinedFunction = udf(computeSentimentWrapper)

}
