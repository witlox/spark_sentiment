package ch.uzh.sentiment.ml

import ch.uzh.sentiment.utils.Timing
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, expr}

object MaximumEntropy {

  val log: Logger = LogManager.getLogger(getClass.getName)

  def execute(timer: Timing, idf_pipeline: PipelineModel, training_df: DataFrame, validation_df: DataFrame, score: String, tfidf: String): (PipelineModel, Double) = {
    log.info("selected Maximum Entropy")

    val en_rf = new RandomForestClassifier().setLabelCol(score).setFeaturesCol(tfidf).setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)

    val en_rf_pipeline = timer.time("staging en_rf pipeline", {
      new Pipeline().setStages(Array(idf_pipeline, en_rf)).fit(training_df)
    })

    log.info("********* Validating **********")

    val staticPrecision = en_rf_pipeline.transform(validation_df).select(avg(expr("double(prediction = score)"))).head.getDouble(0)

    log.info("Precision is with fixed maxBins (20), maxDepth (3) and impurity (gini) " + math.round(staticPrecision * 100) + "%")

    val en_rf_estimator = new Pipeline().setStages(Array(idf_pipeline, en_rf))
    val grid = new ParamGridBuilder().addGrid(en_rf.maxBins, Array(25, 28, 31)).addGrid(en_rf.maxDepth, Array(4, 6, 8)).addGrid(en_rf.impurity, Array("entropy", "gini")).build()

    log.info("Building parameter grid and executing sweep to find best fitting smoothness")

    val all_models = for (i <- grid.indices) yield en_rf_estimator.fit(training_df, grid(i))

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
      (en_rf_pipeline, staticPrecision)
    }
    selection
  }
}
