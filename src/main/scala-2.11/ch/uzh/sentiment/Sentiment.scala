package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Detection, Helper, IO, WordList}
import com.databricks.spark.corenlp.functions._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OptionParser

object Sentiment {

  val log: Logger = LogManager.getLogger(getClass.getName)

  val outputColumn = "filtered"

  val parser = new OptionParser[Config]("scopt") {
    head("sentiment", "0.2")

    opt[Seq[String]]("inputs") required() valueName "<file>,<dir>,..." action { (x, c) => c.copy(inputs = x) } text "inputs to analyze"
    opt[String]("model") valueName "<path>" action { (x, c) => c.copy(model = Some(x)) } text "path to save the model to/load the model from"
    opt[String]("classifier") valueName "<classifier>" action { (x, c) => c.copy(classifier = Some(x)) } text "classifier to use during training: logistic, naivebayes, maxentropy or svm (not setting this will run all classifiers = SLOW)"
    opt[String]("filetype") valueName "file type" action { (x, c) => c.copy(inputFileType = Some(x)) } text "input files type (json, csv, txt, parquet)"
    opt[String]("column") valueName "column" action { (x, c) => c.copy(column = Some(x)) } text "column that contains the text"
    opt[String]("output") valueName "<file>" action { (x, c) => c.copy(output = Some(x)) } text "output to write to, note that the output format is same as input format"
    opt[String]("method") valueName "method" action { (x, c) => c.copy(method = Some(x)) } text "methods: word-score (default), mlib, our-nlp, databricks-nlp"
    opt[Int]("limit") action { (x, c) => c.copy(limit = x) } text "use this number as sample size for detection (and limit/10 is the display count)"
    opt[Unit]("stem") action { (x, c) => c.copy(stem = false) } text "use porter stemmer on source data (default = true, disable when training word-score)"
    opt[Unit]("train") action { (_, c) => c.copy(train = true) } text "train model using input file"
    opt[Unit]("verbose") action { (_, c) => c.copy(verbose = true) } text "let's be very chatty (note that setting this will slow down everything)"
    opt[Unit]("very-verbose") action { (_, c) => c.copy(very_verbose = true) } text "let's all be very very chatty (note that setting this will severely slow down everything)"

    help("help") text "Analyze tweet sentiment"
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        val verbosity = config.verbose || config.very_verbose
        if (verbosity) {
          LogManager.getLogger("ch.uzh").setLevel(Level.DEBUG)
        } else {
          LogManager.getLogger("ch.uzh").setLevel(Level.INFO)
        }
        if (config.very_verbose) {
          LogManager.getRootLogger.setLevel(Level.DEBUG)
        }
        val modelPath = if (config.model.isDefined) {
          config.model.get
        } else {
          "sentiment.model"
        }
        val classifier = if (config.classifier.isDefined) {
          config.classifier.get
        } else {
          "all"
        }
        val spark = SparkSession
          .builder
          .appName("uzhTweetSentiment")
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.shuffle.service.enabled","true")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        import spark.implicits._

        if (config.train) {
          if (config.method.isEmpty || config.method.get.toLowerCase == "word-score") {
            log.info("trying to generate word classification lists")
            val sources = config.inputs.map(i => IO.loadFile(config.inputFileType, i, spark))
            val valueMaps = sources.flatMap { s =>
              val column = Detection.detectTextColumn(s.get._1, config.limit).get
              val cleaned = Helper.cleanSource(column, outputColumn, s.get._1, stem = true)
              CreateScoreList.score(spark, cleaned, outputColumn, config.limit)
            }

            valueMaps.distinct.foreach(vm =>
              spark.sparkContext.parallelize(vm._2)
                .repartition(1)
                .saveAsTextFile("wl" + vm._1 + ".txt")
            )

          } else {
            log.info("constructing training data")
            val tSet = TrainingSet.getTrainingAndTestingDataFrames(config.inputs, config.inputFileType, config.limit, verbosity, spark)
            if (tSet.isEmpty) {
              log.error("could not detect training data")
              sys.exit(-3)
            }
            log.info("cleaning data and paths")
            val training = tSet.map(t => Helper.cleanSource(Detection.detectTextColumn(t, config.limit).get, outputColumn, t, config.stem)).get
            Helper.clean(modelPath, spark)
            log.info("training machine learning model with classifier " + classifier)
            val (model, name, precision) = MlLibSentimentAnalyser.train(training, outputColumn, config.verbose, config.very_verbose, config.limit, classifier)
            model.save(modelPath)
            log.info("saved " + name + " with precision " + math.round(precision * 100) + "% to " + modelPath)
          }
        } else {

          if (config.output.isDefined) {
            Helper.clean(config.output.get, spark)
          }

          log.info("loading files from " + config.inputs.foreach(println))
          val sources = config.inputs.map(i => (i, IO.loadFile(config.inputFileType, i, spark)))
          for ((name, source) <- sources) {
            if (source.isEmpty) {
              log.error("could not load source " + name + " unknown/unsupported filetype")
            } else {

              val data = source.get._1
              val dtype = source.get._2
              val column = if (config.column.isEmpty) {
                  Detection.detectTextColumn(data, config.limit)
              } else {
                config.column
              }
              if (column.isEmpty) {
                log.error("could not find a text column to analyse for " + name)
              } else {
                log.info("selected column: " + column.get)
                if (config.very_verbose) {
                  log.debug("Got " + data.count() + " lines.")
                  display(math.ceil(config.limit/10).toInt, column.get, None, data)
                }

                val cleaned = Helper.cleanSource(column.get, outputColumn, data, config.stem)
                if (verbosity) {
                  log.debug("cleaned source data:")
                  cleaned.show(math.ceil(config.limit/10).toInt)
                }

                if (config.method.isEmpty || config.method.get.toLowerCase == "word-score") {
                  val wl = spark.sparkContext.broadcast(new WordList())
                  log.info("building word score")
                  val output = cleaned.withColumn("computed", new PlainTextAnalyser(wl).computeSentimentUDF(col(outputColumn)))
                  process(config, output, column.get, Some("computed"), dtype)
                } else if (config.method.get.toLowerCase == "mlib") {
                  log.info("loading MLib model from " + modelPath)
                  val model = MlLibSentimentAnalyser.load(modelPath)
                  log.info("select sentiment data using MLib")
                  val output = model.transform(cleaned)
                  process(config, output.toDF, column.get, Some("score"), dtype)
                } else if (config.method.get.toLowerCase == "our-nlp") {
                  log.info("select sentiment data using our CoreNLP method")
                  val output = cleaned.withColumn("computed", new CoreNLPSentimentAnalyzer().computeSentimentUDF(col(outputColumn)))
                  process(config, output.toDF, column.get, Some("computed"), dtype)
                } else {
                  log.info("select sentiment data CoreNLP databricks")
                  val output = cleaned.withColumn("sentiment", sentiment(Symbol(outputColumn)))
                  process(config, output, column.get, Some("computed"), dtype)
                }
              }
            }
          }
        }
        log.info("Done")
        spark.stop()
      case None =>
        log.error("invalid program options")
        sys.exit(-1)
    }
  }

  private def display(count: Int, textColumn: String, scoreColumn: Option[String], data: DataFrame) = {
    if (count == 0) {
      log.info("displaying contents:")
      if (scoreColumn.isDefined) {
        log.info(data.select(col(textColumn), col(scoreColumn.get)).collect.foreach(println))
      } else {
        log.info(data.select(col(textColumn)).collect.foreach(println))
      }
    } else {
      log.info("displaying first " + count + " contents:")
      if (scoreColumn.isDefined) {
        log.info(data.select(col(textColumn), col(scoreColumn.get)).take(count).foreach(println))
      } else {
        log.info(data.select(col(textColumn)).take(count).foreach(println))
      }
    }
  }

  private def process(config: Config, output: DataFrame, textColumn: String, scoreColumn: Option[String], dtype: String) = {
    val data = if (!config.very_verbose) {
      output.drop("filtered").drop("words").drop("tf").drop("tfidf").drop("rawPrediction").drop("probability")
    } else output
    display(math.ceil(config.limit/10).toInt, textColumn, scoreColumn, data)
    if (config.output.isDefined) {
      log.info("saving sentiments")
      IO.save(dtype, data, config.output.get)
    }
  }

  case class Config(inputs: Seq[String] = Seq(),
                    model: Option[String] = None,
                    classifier: Option[String] = None,
                    inputFileType: Option[String] = None,
                    output: Option[String] = None,
                    column: Option[String] = None,
                    method: Option[String] = None,
                    limit: Int = 100,
                    train: Boolean = false,
                    stem: Boolean = true,
                    verbose: Boolean = false,
                    very_verbose: Boolean = false)

}