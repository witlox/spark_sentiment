package ch.uzh.sentiment.utils

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class Emoji(spark: SparkSession) {

  private val happy = List(":-)", ":)", ";)", ":o)", ":]", ":3", ":c)", ":>", "=]", "8)", "=)", ":}",
    ":^)", ":-D", ":D", "8-D", "8D", "x-D", "xD", "X-D", "XD", "=-D", "=D",
    "=-3", "=3", ":-))", ":'-)", ":')", ":*", ":^*", ">:P", ":-P", ":P", "X-P",
    "x-p", "xp", "XP", ":-p", ":p", "=p", ":-b", ":b", ">:)", ">;)", ">:-)",
    "<3")

  private val sad = List(":L", ":-/", ">:/", ":S", ">:[", ":@", ":-(", ":[", ":-||", "=L", ":<",
    ":-[", ":-<", "=\\", "=/", ">:(", ":(", ">.<", ":'-(", ":'(", ":\\", ":-c",
    ":c", ":{", ">:\\", ";(")


  /***
    * convert emojis to there corresponding english identifier
    * @return Map of [emoji as string, english expression]
    */
  def get: Map[String, String] = {
    val wordList: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    if (spark != null) {
      IO.loadEmojiCsvResource(spark).foreach(rowLine => {
        wordList.put(rowLine.getString(1), rowLine.getString(0))
      })
    }
    for (h <- happy) {
      wordList.put(h, "happy")
    }
    for (s <- sad) {
      wordList.put(s, "sad")
    }
    Map(wordList.toSeq: _*)
  }

}
