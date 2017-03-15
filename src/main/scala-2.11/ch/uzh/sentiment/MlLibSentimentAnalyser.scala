package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Detection, Timing}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MlLibSentimentAnalyser {

  val log: Logger = LogManager.getLogger(getClass.getName)

  val score = "score"
  val tfidf = "tfidf"

  def train(spark: SparkSession, training: DataFrame, textColumn: String, verbose: Boolean, very_verbose: Boolean, limit: Int, classifier: String) : (PipelineModel, String, Double) = {
    val timer = new Timing

    val (labellingColumn, idf_pipeline, training_df, validation_df, testing_df) = prepare(training, textColumn, very_verbose, limit, timer)

    val (selection, precision, name) = classifier match {
      case "logistic" =>
        log.debug("executing logistic regression pipeline")
        val r = ml.Regression.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        (r._1, r._2, "logistic regression")
      case "naivebayes" =>
        log.debug("executing naive bayes pipeline")
        val r = ml.NaiveBayes.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        (r._1, r._2, "naive bayes")
      case "maxentropy" =>
        log.debug("executing random forrest pipeline")
        val r = ml.MaximumEntropy.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        (r._1, r._2, "maximum entropy")
      case _ =>
        val regression = ml.Regression.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        val naivebayes = ml.NaiveBayes.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        val maxentropy = ml.MaximumEntropy.execute(timer, idf_pipeline, training_df, validation_df, score, tfidf)
        log.info("------------- All classifiers have been run ----------------")
        log.info("Logistic Regression: " + regression._2)
        log.info("Naive Bayes: " + naivebayes._2)
        log.info("Maximum Entropy: " + maxentropy._2)
        log.info("------------- Selecting fastest ----------------")
        if (regression._2 >= naivebayes._2 && regression._2 >= maxentropy._2) {
          log.info("selected Logistic Regression")
          (regression._1, regression._2, "logistic regression")
        } else if (naivebayes._2 >= maxentropy._2) {
          log.info("selected Naive Bayes")
          (naivebayes._1, naivebayes._2, "naive bayes")
        } else {
          log.info("selected Maximum Entropy")
          (maxentropy._1, maxentropy._2, "maximum entropy")
        }
    }

    log.info("********* Testing **********")

    val result_df = selection.transform(testing_df)

    log.info("Testing content: ")
    log.info(result_df.take(math.ceil(limit/10).toInt).foreach(println))

    def getRandom (dataset : DataFrame, n : Int) = {
      val count = dataset.count
      val m = if (count > n) n else count
      dataset.sample(withReplacement = true, 1.0*(m/count)).limit(n)
    }

    log.info("Testing sample results: ")
    getRandom(result_df, math.ceil(limit/10).toInt).foreach(x => {
      log.info("---------------------------------------------------------------")
      log.info("Text = " + x.getAs[String](x.fieldIndex("text")))
      log.info("Actual Label = " + x.getAs[String](x.fieldIndex(labellingColumn)))
      log.info("Predicted Label = " + x.getAs[String](x.fieldIndex("prediction")))
      log.info("----------------------------------------------------------------\n\n")
    })

    if (precision < 65.0) log.warn("Precision SUCKS!")
    else if (precision < 75.0) log.warn("Precision is not that good")
    else if (precision < 80.0) log.info("Precision is not that bad")
    else log.info("Precision is awesomepants")

    (selection, name, precision)
  }

  private def prepare(training: DataFrame, textColumn: String, very_verbose: Boolean, limit: Int, timer: Timing) = {
    log.info("********* Setting up training ***********")

    val (t, labellingColumn) = if (Detection.detectValueColumn(training, limit).isEmpty) {
      log.debug("trying to get categories")
      val catCol = Detection.detectCategoricalColumn(training, limit).get
      log.debug("found categorical column " + catCol + ", converting if necessary")
      Detection.convertCategoricalColumn(training, catCol)
    } else {
      log.info("getting values")
      (training, Detection.detectValueColumn(training, limit).get)
    }
    log.debug("detected labeling column " + labellingColumn + ", duplicating it to 'score' column")
    val data = t.withColumn(score, col(labellingColumn))

    log.info("********* Training **********")

    log.debug("creating count vectorizer")

    val tokenizer = timer.time("tokenizing", {
      new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol(textColumn).setOutputCol("words")
    })

    val vectorizer = timer.time("vectorizing", {
      new CountVectorizer().setMinTF(1.0).setMinDF(5.0).setVocabSize(math.pow(2, 17).toInt).setInputCol("words").setOutputCol("tf")
    })

    val cv_pipeline = timer.time("staging cv pipeline", {
      new Pipeline().setStages(Array(tokenizer, vectorizer))
    })

    val idf = new IDF().setInputCol("tf").setOutputCol(tfidf)

    val idf_pipeline = timer.time("staging idf pipeline", {
      new Pipeline().setStages(Array(cv_pipeline, idf)).fit(data)
    })

    log.info("splitting into training, validation and testing set")

    val splits = timer.time("splitting dataset", {
      data.randomSplit(Array(0.7, 0.2, 0.1), 0)
    })
    val (training_df, validation_df, testing_df) = (splits(0), splits(1), splits(2))

    if (very_verbose) {
      log.debug("splitted data in training: " + training_df.count + ", validation: " + validation_df.count + " and testing: " + testing_df.count)
    }
    (labellingColumn, idf_pipeline, training_df, validation_df, testing_df)
  }

  def load(path: String): PipelineModel = {
    PipelineModel.load(path)
  }
}
