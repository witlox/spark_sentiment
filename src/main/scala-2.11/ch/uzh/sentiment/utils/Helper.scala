package ch.uzh.sentiment.utils

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Helper {
  val log: Logger = LogManager.getLogger(getClass.getName)

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

  def cleanSource(inputColumn: String, outputColumn: String, df: DataFrame) : DataFrame = {
    val cleaned = df.where(df.col(inputColumn).isNotNull).filter(r => stemString(cleanString(r.getAs[String](inputColumn))).nonEmpty)
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol(inputColumn)
      .setOutputCol("tokens_raw")
    val remover = new StopWordsRemover()
      .setInputCol("tokens_raw")
      .setCaseSensitive(false)
      .setOutputCol("tokens_clean")
    val intermediate = remover.transform(tokenizer.transform(cleaned)).filter(o => o.getAs[Seq[String]]("tokens_clean").nonEmpty).na.drop
    intermediate.withColumn(outputColumn, concat_ws(" ", col("tokens_clean"))).drop("tokens_raw").drop("tokens_clean")
  }

  def cleanString(text: String): String = {
    text.toLowerCase()
      .replaceAll("\"", "").replaceAll("'", "")
      .replaceAll("\n", "").replaceAll("\r\n", "").replaceAll("\t", "")
      .replaceAll("rt\\s+", "").replaceAll("@\\w+", "").replaceAll("#", "")
      .replaceAll("http\\S+", "")
      .replaceAll(" +", " ").replaceAll("\\s+", " ")
  }

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

  def clean(session: SparkSession, path: String) {
    if (check(session, path)) {
      log.info("file " + path + " exists, deleting")
      if (path.startsWith("hdfs:")) {
        val fs = org.apache.hadoop.fs.FileSystem.get(session.sparkContext.hadoopConfiguration)
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

  def check(session: SparkSession, path: String): Boolean = {
    log.info("check if file " + path + " exists")
    if (path.startsWith("hdfs:")) {
      val fs = org.apache.hadoop.fs.FileSystem.get(session.sparkContext.hadoopConfiguration)
      val fs_path = new org.apache.hadoop.fs.Path(path)
      fs.exists(fs_path)
    } else {
      val f = Paths.get(path)
      Files.exists(f)
    }
  }
}
