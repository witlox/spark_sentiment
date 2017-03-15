package ch.uzh.sentiment.ml

import ch.uzh.sentiment.utils.Timing
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, expr}

object Regression {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def execute(timer: Timing, idf_pipeline: PipelineModel, training_df: DataFrame, validation_df: DataFrame, score: String, tfidf: String): (PipelineModel, Double) = {
    log.info("selected Logistic Regression")

    val lambda = 0.02
    val alpha = 0.3

    log.debug("fit elatic net regularization")
    val en_lr = new LogisticRegression().setLabelCol(score).setFeaturesCol(tfidf).setRegParam(lambda).setMaxIter(100).setElasticNetParam(alpha)

    val en_lr_pipeline = timer.time("staging en_lr pipeline", {
      new Pipeline().setStages(Array(idf_pipeline, en_lr)).fit(training_df)
    })

    log.info("********* Validating **********")

    val staticPrecision = en_lr_pipeline.transform(validation_df).select(avg(expr("double(prediction = score)"))).head.getDouble(0)

    log.info("Precision is with fixed λ(" + lambda + ") and α(" + alpha + ") = " + math.round(staticPrecision * 100) + "%")

    val en_lr_estimator = new Pipeline().setStages(Array(idf_pipeline, en_lr))
    val grid = new ParamGridBuilder().addGrid(en_lr.regParam, Array(0.0, 0.01, 0.02)).addGrid(en_lr.elasticNetParam, Array(0.0, 0.2, 0.4)).build()

    log.info("Building parameter grid and executing sweep to find best fitting λ and α")

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
      (en_lr_pipeline, staticPrecision)
    }
    selection
  }
}
