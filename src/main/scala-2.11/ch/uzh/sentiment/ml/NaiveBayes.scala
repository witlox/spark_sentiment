package ch.uzh.sentiment.ml

import ch.uzh.sentiment.utils.Timing
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, expr}

object NaiveBayes {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def execute(timer: Timing, idf_pipeline: PipelineModel, training_df: DataFrame, validation_df: DataFrame, score: String, tfidf: String): (PipelineModel, Double) = {
    log.info("selected Naive Bayes")

    val smoothing = 1.0
    val en_nb = new NaiveBayes().setLabelCol(score).setFeaturesCol(tfidf).setSmoothing(smoothing).setModelType("multinomial")

    val en_nb_pipeline = timer.time("staging en_nb pipeline", {
      new Pipeline().setStages(Array(idf_pipeline, en_nb)).fit(training_df)
    })

    log.info("********* Validating **********")

    val staticPrecision = en_nb_pipeline.transform(validation_df).select(avg(expr("double(prediction = score)"))).head.getDouble(0)

    log.info("Precision is with fixed smoothing (" + smoothing + ") = " + math.round(staticPrecision * 100) + "%")

    val en_lr_estimator = new Pipeline().setStages(Array(idf_pipeline, en_nb))
    val grid = new ParamGridBuilder().addGrid(en_nb.smoothing, Array(0.85, 0.90, 0.95, 1.05, 1.1, 1.15)).build()

    log.info("Building parameter grid and executing sweep to find best fitting smoothness")

    val all_models = for (i <- grid.indices) yield en_lr_estimator.fit(training_df, grid(i))

    val accuracies = for (m <- all_models) yield (m.transform(validation_df).select(avg(expr("double(score = prediction)"))).head.getDouble(0), m)

    val (dynamicPrecision, best_model) = accuracies.maxBy(_._1)

    log.info("Precision after sweep = " + math.round(dynamicPrecision*100) + "%")

    val difference = dynamicPrecision - staticPrecision
    log.info("Difference between static model and model after parameter sweep: " + f"$difference%1.2f" + "%")

    val selection = if (difference > 0) {
      log.debug("selecting dynamic model")
      (best_model, dynamicPrecision)
    } else {
      log.warn("selecting static model")
      (en_nb_pipeline, staticPrecision)
    }
    selection
  }
}
