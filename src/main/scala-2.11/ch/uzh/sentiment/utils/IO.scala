package ch.uzh.sentiment.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {
  private def findDelimiter(line1: String, line2: String): String = {
    if (line1.count(_ == ",") == line2.count(_ == ",")) ","
    else if (line1.count(_ == "|") == line2.count(_ == "|")) "|"
    else if (line1.count(_ == ":") == line2.count(_ == ":")) ":"
    else if (line1.count(_ == "\t") == line2.count(_ == ",")) "\t"
    else ";"
  }

  private def load(inputType: String, path: String, spark:SparkSession): Option[(DataFrame, String)] = inputType match  {
    case "json" => Some(spark.read.json(path), "json")
    case "csv" =>
      val firstTwoLines = spark.read.text(path).take(2)
      val delim = findDelimiter(firstTwoLines.head.mkString, firstTwoLines.tail.mkString)
      val firstTwoLinesCsv = spark.read.format("com.databricks.spark.csv").option("delimiter", delim).load(path).take(2)
      val header = firstTwoLinesCsv.head.schema == firstTwoLinesCsv.tail.head.schema
      Some(spark.read
        .format("com.databricks.spark.csv")
        .option("delimiter", delim)
        .option("header", header)
        .option("inferSchema", "true")
        .load(path), "csv")
    case "txt" => Some(spark.read.text(path), "txt")
    case "parquet" => Some(spark.read.parquet(path), "par")
    case _ => None
  }

  private def loadFileFromPath(path: String, spark: SparkSession) = path.toLowerCase match {
    case json if json.contains(".json") => load("json", path, spark)
    case txt if txt.contains(".txt") => load("txt", path, spark)
    case csv if csv.contains(".csv") => load("csv", path, spark)
    case par if par.contains(".par") => load("parquet", path, spark)
    case _ => None
  }

  def loadFile(fileType: Option[String], path: String, spark: SparkSession): Option[(DataFrame, String)] = fileType match {
    case Some(ift) => load(ift.toLowerCase, path, spark)
    case None => loadFileFromPath(path, spark)
  }

  def save(fileType: String, df: DataFrame, output: String) = fileType match {
    case "json" => df.write.json(output)
    case "csv" => df.write.format("com.databricks.spark.csv").save(output)
    case "txt" => df.write.text(output)
    case "parquet" => df.write.save(output)
  }
}
