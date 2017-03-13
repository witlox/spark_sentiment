package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Detection, Timing}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MlLibSentimentAnalyser {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def train(spark: SparkSession, training: DataFrame, textColumn: String, verbose: Boolean, very_verbose: Boolean, limit: Int) : PipelineModel = {
    val timer = new Timing(verbose || very_verbose)

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
    val data = t.withColumn("score", col(labellingColumn))

    log.info("********* Training **********")

    log.debug("creating count vectorizer")

    val tokenizer = timer.time("tokenizing", { new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol(textColumn).setOutputCol("words") })

    val vectorizer = timer.time("vectorizing", { new CountVectorizer().setMinTF(1.0).setMinDF(5.0).setVocabSize(math.pow(2, 17).toInt).setInputCol("words").setOutputCol("tf") })

    val cv_pipeline = timer.time("staging cv pipeline", { new Pipeline().setStages(Array(tokenizer, vectorizer)) })

    val idf = new IDF().setInputCol("tf").setOutputCol("tfidf")

    val idf_pipeline = timer.time("staging idf pipeline", { new Pipeline().setStages(Array(cv_pipeline, idf)).fit(data) })

    log.info("splitting into training, validation and testing set")

    val splits = timer.time("splitting dataset", { data.randomSplit(Array(0.7, 0.2, 0.1), 0) })
    val (training_df, validation_df, testing_df) = (splits(0), splits(1), splits(2))

    if (very_verbose) {
      log.debug("splitted data in training: " + training_df.count + ", validation: " + validation_df.count + " and testing: " + testing_df.count)
    }

    log.debug("fit elatic net regularization")

    val lambda = 0.02
    val alpha = 0.3

    val en_lr = new LogisticRegression().setLabelCol("score").setFeaturesCol("tfidf").setRegParam(lambda).setMaxIter(100).setElasticNetParam(alpha)

    val en_lr_pipeline = timer.time("staging en_lr pipeline", { new Pipeline().setStages(Array(idf_pipeline, en_lr)).fit(training_df) })

    log.info("********* Validating **********")

    val precision = en_lr_pipeline.transform(validation_df).select(avg(expr("double(prediction = score)"))).head.getDouble(0)

    log.info("Precision is with fixed λ(" + lambda + ") and α(" + alpha + ") = " + math.round(precision*100) + "%")

    val en_lr_estimator = new Pipeline().setStages(Array(idf_pipeline, en_lr))
    val grid = new ParamGridBuilder().addGrid(en_lr.regParam, Array(0.0, 0.01, 0.02)).addGrid(en_lr.elasticNetParam, Array(0.0, 0.2, 0.4)).build()

    log.info("Building parameter grid and executing sweep to find best fitting λ and α")

    val all_models = for (i <- 0 to grid.length - 1) yield en_lr_estimator.fit(training_df, grid(i))

    val accuracies = for (m <- all_models) yield (m.transform(validation_df).select(avg(expr("double(score = prediction)"))).head.getDouble(0), m)

    val (accuracy, best_model) = accuracies.maxBy(_._1)

    log.info("Precision after sweep = " + math.round(accuracy*100) + "%")

    log.info("********* Testing **********")

    val result_df = best_model.transform(testing_df)

    log.info("Testing content: ")
    log.info(result_df.take(math.ceil(limit/10).toInt).foreach(println))

    def getRandom (dataset : DataFrame, n : Int) = {
      val count = dataset.count
      val m = if (count > n) n else count
      dataset.sample(withReplacement = true, 1.0*(m/count)).limit(n)
    }

    log.info("Testing sample results: ")
    getRandom(result_df, math.ceil(limit/10).toInt).foreach( x => {
      log.info("---------------------------------------------------------------")
      log.info("Text = " + x.getAs[String](x.fieldIndex("text")))
      log.info("Actual Label = " + x.getAs[String](x.fieldIndex(labellingColumn)))
      log.info("Predicted Label = " + x.getAs[String](x.fieldIndex("prediction")))
      log.info("----------------------------------------------------------------\n\n")
    } )

    if (accuracy < 70.0) log.warn("Precision SUCKS!")
    else if (accuracy < 80.0) log.warn("Precision is not that good")
    else if (accuracy < 85.0) log.info("Precision is not that bad")
    else log.info("Precision is awesomepants")

    best_model
  }

  def load(path: String): PipelineModel = {
    PipelineModel.load(path)
  }
}
