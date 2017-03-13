package ch.uzh.sentiment.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, size, split}
import ch.uzh.sentiment.utils.Helper._
import org.apache.spark.sql.functions._

object Detection {

  val categoricalSizeLimit = 5
  val minimumTextSize = 5

  private def detectStringColumns(df: DataFrame) = (for {
    d_type <- df.dtypes if d_type._2 == "StringType"
  } yield d_type._1).toList

  private def detectIntegerColumns(df: DataFrame) = (for {
    d_type <- df.dtypes if d_type._2 == "IntegerType"
  } yield d_type._1).toList

  private def detectDoubleColumns(df: DataFrame) = (for {
    d_type <- df.dtypes if d_type._2 == "DoubleType"
  } yield d_type._1).toList


  private def averageWordCount(df: DataFrame, column: String, limit: Int) = df.withColumn("wc", size(split(col(column), " "))).limit(limit).agg(avg("wc")).first().getDouble(0)

  private def detectTextColumns(df: DataFrame, limit: Int) = {
    def builder(expr: => List[String], acc: Map[String, Double]): Map[String, Double] = expr match {
      case Nil => acc
      case w :: tail => builder(tail, acc + (w -> averageWordCount(df, w, limit)) )
    }
    builder(detectStringColumns(df), Map())
  }

  def detectTextColumn(df: DataFrame, limit: Int): Option[String] = detectTextColumns(df, limit) match {
    case x if x.nonEmpty && x.exists(c => c._2 > minimumTextSize.toDouble) => Some(x.toSeq.sortWith(_._2 > _._2).head._1)
    case _ => None
  }

  private def detectRange(df: DataFrame, column: String, limit: Int) = df.select(column).take(limit).toSet.size == df.select(column).take(limit).length

  private def detectIndexColumns(df: DataFrame, limit: Int) = {
    def builder(expr: => List[String], acc: List[String]): List[String] = expr match {
      case Nil => acc
      case w :: tail if detectRange(df, w, limit) => builder(tail, w :: acc)
      case w :: tail if !detectRange(df, w, limit) => builder(tail, acc)
    }
    builder(detectIntegerColumns(df), Nil)
  }

  def detectIndexColumn(df: DataFrame, limit: Int) : Option[String] = detectIndexColumns(df, limit) match {
    case x if x.nonEmpty && x.size == 1 => Some(x.head)
    case _ => None
  }

  private def detectCategorical(df: DataFrame, column: String, limit: Int) = {
    (df.select(column).take(limit).toSet.size < limit) &&
      (df.select(column).take(limit).toSet.size < categoricalSizeLimit) &&
      (df.select(column).take(limit).toSet.size > 1)
  }

  private def detectCategoricalColumns(df: DataFrame, limit: Int) = {
    def builderForInts(expr: => List[String], acc: List[String]): List[String] = expr match {
      case Nil => acc
      case w :: tail if detectCategorical(df, w, limit) => builderForInts(tail, w :: acc)
      case w :: tail if !detectCategorical(df, w, limit) => builderForInts(tail, acc)
    }
    def builderForStrings(expr: => List[String], acc: List[String]): List[String] = expr match {
      case Nil => acc
      case w :: tail if averageWordCount(df, w, limit).round.toInt == 1 => builderForStrings(tail, w :: acc)
      case w :: tail if averageWordCount(df, w, limit).round.toInt != 1 => builderForStrings(tail, acc)
    }
    (builderForInts(detectIntegerColumns(df), Nil), builderForStrings(detectStringColumns(df), Nil))
  }

  def detectCategoricalColumn(df: DataFrame, limit: Int) : Option[(String)] = detectCategoricalColumns(df, limit) match {
    case (i, _) if i.nonEmpty && i.size == 1 => Some(i.head)
    case (_, s) if s.nonEmpty && s.size == 1 => Some(s.head)
    case (_, s) if s.nonEmpty && s.size > 1 =>
      val intersect = detectValueInStringColumns(df).intersect(s)
      if (intersect.size == 1) {
        Some(intersect.head)
      } else {
        Some(s.head)
      }
    case _ => None
  }

  def convertCategoricalColumn(df: DataFrame, column: String): (DataFrame, String) = {
    if (detectIntegerColumns(df).contains(column)) {
      (df, column)
    } else {
      val outputColumn = "converted_categorical"
      val indexedWords = df.select(column).distinct.collect.zipWithIndex.map(t => (t._1.getString(0), t._2)).toMap
      val encoder: (String => Int) = (key: String) => indexedWords.get(key).head
      val encoderUDF = udf(encoder)
      (df.withColumn("our_categorical", encoderUDF(col(column))), outputColumn)
    }
  }

  private def detectValueInStringColumns(df: DataFrame) = for {
    c <- detectStringColumns(df)
    if df.select(c).head.getString(0).isNumeric
  } yield c

  def detectValueColumn(df: DataFrame, limit: Int) : Option[String] = {
    val dCols = detectDoubleColumns(df)
    if (dCols.nonEmpty){
      if (dCols.size == 1) {
        Some(dCols.head)
      } else {
        None
      }
    } else {
      val cvCols = detectValueInStringColumns(df)
      if (cvCols.nonEmpty) {
        if (cvCols.size == 1) {
          Some(cvCols.head)
        } else {
          None
        }
      } else {
        None
      }
    }
  }
}
