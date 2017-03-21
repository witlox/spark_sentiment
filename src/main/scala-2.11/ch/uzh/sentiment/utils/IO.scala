package ch.uzh.sentiment.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source

object IO {

  private def findDelimiter(path: String, spark: SparkSession): String = {
    val firstTwoLines = spark.read.text(path).take(2)
    val line1 = firstTwoLines.head.mkString
    val line2 = firstTwoLines.tail.mkString
    if (line1.count(_ == ",") == line2.count(_ == ",")) ","
    else if (line1.count(_ == "|") == line2.count(_ == "|")) "|"
    else if (line1.count(_ == ":") == line2.count(_ == ":")) ":"
    else if (line1.count(_ == "\t") == line2.count(_ == ",")) "\t"
    else ";"
  }

  private def detectEscape(path: String, delim: String, spark: SparkSession): Option[String] = {
    val line = spark.read.text(path).take(2).tail.mkString
    val txts = for {part <- line.split(delim) if part.split(" ").length > 3} yield part
    if (txts.length > 0) {
      val txt = txts.maxBy(_.length)
      if (txt.charAt(0) == '\"') Some("\"")
      else if (txt.charAt(0) == '\'') Some("'")
      else None
    } else {
      None
    }
  }

  private def hasHeader(path: String, delim: String, spark: SparkSession): Boolean = {
    val firstTwoLinesCsv = spark.read.format("com.databricks.spark.csv").option("delimiter", delim).load(path).take(2)
    firstTwoLinesCsv.head.schema == firstTwoLinesCsv.tail.head.schema
  }

  private def csvLoad(path: String, header: Boolean, delim: String, spark: SparkSession) = detectEscape(path, delim, spark) match {
    case Some(x) => spark.read.format("com.databricks.spark.csv").option("delimiter", delim).option("escape", x).option("header", header).option("inferSchema", "true").load(path)
    case None => spark.read.format("com.databricks.spark.csv").option("delimiter", delim).option("header", header).option("inferSchema", "true").load(path)
  }

  private def load(inputType: String, path: String, spark: SparkSession): Option[(DataFrame, String)] = inputType match  {
    case "json" => Some(spark.read.json(path), "json")
    case "csv" =>
      val delim = findDelimiter(path, spark)
      val header = hasHeader(path, delim, spark)
      Some(csvLoad(path, header, delim, spark), "csv")
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

  def save(fileType: String, df: DataFrame, output: String): Unit = fileType match {
    case "json" => df.write.json(output)
    case "csv" => df.write.format("com.databricks.spark.csv").save(output)
    case "txt" => df.write.text(output)
    case "parquet" => df.write.save(output)
  }

  def loadEmojiCsvResource(spark: SparkSession): DataFrame = {
    val csvRDD = spark.sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/emojis.csv")).getLines.drop(1).toList)
    val b = csvRDD.map(s => (s.split(";")(0).replace("\"", ""), s.split(";")(1).replace("\"", "")))
    spark.createDataFrame(b).na.drop
  }

  def loadWordPositiveWordListResource: List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/positive.txt")).getLines.toList
  }

  def loadWordNegativeWordListResource: List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/negative.txt")).getLines.toList
  }

}
