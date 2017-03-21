package ch.uzh.sentiment.utils

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object Helper {
  val log: Logger = LogManager.getLogger(getClass.getName)

  @transient private var emojiMap: Map[String, String] = _

  implicit class OpsNum(val str: String) extends AnyVal {
    def isNumeric: Boolean = scala.util.Try(str.toDouble).isSuccess
  }

  implicit class OpsConv(val value: Any) extends AnyVal {
    def convertToDouble: Double = value match {
      case value: String => value.toDouble
      case value: Integer => value.toDouble
      case value: Double => value
      case _ => 0.0
    }
  }


  private def cleanText: (String => String) = (text: String) => cleanString(emojiToWord(TweetTokenizer.tokenizeToString(text)))
  private def cleanTextAndStem: (String => String) = (text: String) => cleanText(stemString(text))

  def cleanTextUDF: UserDefinedFunction = udf(cleanText)
  def cleanTextAndStemUDF: UserDefinedFunction = udf(cleanTextAndStem)

  /***
    * try to return normalized text from it's raw source
    * @param inputColumn
    * @param outputColumn
    * @param df
    * @return source dataframe with extra column containing cleaned text
    */
  def cleanSource(inputColumn: String, outputColumn: String, df: DataFrame, stem: Boolean) : DataFrame = {
    emojiMap = new Emoji(df.sparkSession).get

    val cleaned = if (stem) {
      df.where(df.col(inputColumn).isNotNull).withColumn("converted_text", cleanTextAndStemUDF(col(inputColumn)))
    } else {
      df.where(df.col(inputColumn).isNotNull).withColumn("converted_text", cleanTextUDF(col(inputColumn)))
    }

    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("converted_text")
      .setOutputCol("tokens_raw")
    val remover = new StopWordsRemover()
      .setInputCol("tokens_raw")
      .setCaseSensitive(false)
      .setOutputCol("tokens_clean")
    val intermediate = remover.transform(tokenizer.transform(cleaned))

    intermediate.withColumn(outputColumn, concat_ws(" ", col("tokens_clean"))).drop("tokens_raw").drop("tokens_clean").drop("converted_text")
  }

  /***
    * remove all useless characters and strings from our source text
    * quotes and double quotes
    * line breaks, carriage returns and tabs
    * retweets, references and prefixed hashtext
    * all uri's
    * and whitespace overhead
    * @param text
    * @return lowercase clean text
    */
  def cleanString(text: String): String = {
    text.toLowerCase
      .replaceAll("\"", "").replaceAll("'", "")
      .replaceAll("\n", "").replaceAll("\r", "").replaceAll("\t", "")
      .replaceAll("rt\\s+", "").replaceAll("@\\w+", "").replaceAll("#", "")
      .replaceAll("http\\S+", "")
      .replaceAll(" +", " ").replaceAll("\\s+", " ")
  }

  /***
    * convert emoji to it's text variant
    * @param text
    * @return converted text
    */
  def emojiToWord(text: String): String = {
    text.split(" ").map(word => if (emojiMap.contains(word)) emojiMap.get(word) else word).mkString(" ")
  }

  /***
    * return only the stems of the words (using Porter stemming)
    * @param text
    * @return stemmed text
    */
  def stemString(text: String): String = {
    text.split(" ").map(word => Stemmer.stem(word)).mkString(" ")
  }

  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      log.error("Unable to delete " + file.getAbsolutePath)
    }
  }

  def clean(path: String, spark: SparkSession) {
    if (check(path, spark)) {
      log.info("file " + path + " exists, deleting")
      if (path.startsWith("hdfs:")) {
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val fs_path = new org.apache.hadoop.fs.Path(path)
        fs.delete(fs_path, true)
      } else {
        val f = Paths.get(path)
        if (Files.isDirectory(f)) {
          deleteRecursively(f.toFile)
        } else {
          Files.delete(f)
        }
      }
    }
  }

  def check(path: String, spark: SparkSession): Boolean = {
    log.info("check if file " + path + " exists")
    if (path.startsWith("hdfs:")) {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val fs_path = new org.apache.hadoop.fs.Path(path)
      fs.exists(fs_path)
    } else {
      val f = Paths.get(path)
      Files.exists(f)
    }
  }

  /***
    * Determine distance between 2 strings
    * @param s1
    * @param s2
    * @return distance count
    */
  def stringDistance(s1: String, s2: String): Int = {
    val memo = mutable.Map[(List[Char], List[Char]), Int]()
    def min(a:Int, b:Int, c:Int) = Math.min( Math.min( a, b ), c)
    def sd(s1: List[Char], s2: List[Char]): Int = {
      if (!memo.contains((s1, s2))) {
        memo((s1, s2)) = (s1, s2) match {
          case (_, Nil) => s1.length
          case (Nil, _) => s2.length
          case (c1 :: t1, c2 :: t2) => min(sd(t1, s2) + 1, sd(s1, t2) + 1, sd(t1, t2) + (if (c1 == c2) 0 else 1))
        }
      }
      memo((s1,s2))
    }
    sd(s1.toList, s2.toList)
  }
}
