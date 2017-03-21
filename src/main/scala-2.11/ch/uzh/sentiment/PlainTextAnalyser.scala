package ch.uzh.sentiment

import ch.uzh.sentiment.utils.WordList
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class PlainTextAnalyser(wordList: Broadcast[WordList]) extends Serializable {

  lazy val wl: WordList = wordList.value

  private def scoreBuilder(expr: => List[String], acc: Double): Double = expr match {
    case Nil => acc
    case List(_) => acc
    case head :: tail => scoreBuilder(tail, acc + wl.value(head))
  }

  private def computeSentiment(text: String): Double = {
    scoreBuilder(text.split(" ").toList, 0.0) / text.split(" ").length
  }

  private def computeSentimentWrapper: (String => Double) = (text: String) => computeSentiment(text)

  def computeSentimentUDF: UserDefinedFunction = udf(computeSentimentWrapper)

}
