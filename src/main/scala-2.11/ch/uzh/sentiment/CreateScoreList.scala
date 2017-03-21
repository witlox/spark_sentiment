package ch.uzh.sentiment

import ch.uzh.sentiment.utils.Detection
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateScoreList {

  val log: Logger = LogManager.getLogger(getClass.getName)

  val hashingTF = new HashingTF()

  def score(spark: SparkSession, source: DataFrame, textColumn: String, limit: Int): Map[Any, Array[String]] = {
    log.info("********* extracting data ***********")

    val (t, labellingColumn) = if (Detection.detectValueColumn(source, limit).isEmpty) {
      log.debug("trying to get categories")
      val catCol = Detection.detectCategoricalColumn(source, limit).get
      log.debug("found categorical column " + catCol + ", converting if necessary")
      Detection.convertCategoricalColumn(source, catCol)
    } else {
      log.info("getting values")
      (source, Detection.detectValueColumn(source, limit).get)
    }
    log.debug("detected labeling column " + labellingColumn)

    val categories = if (Detection.extractIntegerCategories(spark, t, labellingColumn, limit).isDefined) {
      Detection.extractIntegerCategories(spark, t, labellingColumn, limit).get
    } else {
      Detection.extractStringCategories(spark, t, labellingColumn, limit).get
    }

    log.debug("found " + t + " categories: ")
    categories.foreach(c => log.debug(c))

    val allWordsInCategory = categories.map { category =>
      val categorized = t.filter(r => r.get(r.fieldIndex(labellingColumn)) == category).withColumn("split_" + textColumn, split(col(textColumn), " "))
      val vectorized = new CountVectorizer().setMinDF(5).setMinTF(2).setInputCol("split_" + textColumn).fit(categorized)
      category -> vectorized.vocabulary
    }

    val overlap = spark.sparkContext.parallelize(
      allWordsInCategory.flatMap(aw => aw._2).map(word => (word, 1))
    ) .reduceByKey(_+_)
      .filter(k => k._2 > 2)
      .map(r => r._1)
      .collect

    allWordsInCategory.map(a => a._1 -> a._2.filter(e => !overlap.contains(e))).toMap
  }
}
