package ch.uzh.sentiment

import ch.uzh.sentiment.utils.{Detection, Helper, IO}
import com.databricks.spark.corenlp.functions._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object Sentiment {

  val log: Logger = LogManager.getLogger(getClass.getName)

  val outputColumn = "filtered"

  val parser = new OptionParser[Config]("scopt") {
    head("sentiment", "0.2")

    opt[Seq[String]]('i', "inputs") required() valueName "<file>,<dir>,..." action { (x, c) => c.copy(inputs = x) } text "inputs to analyze"
    opt[String]('m', "model") valueName "<path>" action { (x, c) => c.copy(model = x) } text "path to save the model to/load the model from"
    opt[String]("filetype") valueName "file type" action { (x, c) => c.copy(inputFileType = Some(x)) } text "input files type (json, csv, txt, parquet)"
    opt[String]('c', "column") valueName "column" action { (x, c) => c.copy(column = Some(x)) } text "column that contains the text"
    opt[String]('o', "output") valueName "<file>" action { (x, c) => c.copy(output = Some(x)) } text "output to write to, note that the output format is same as input format"
    opt[String]("method") valueName "method" action { (x, c) => c.copy(method = Some(x)) } text "methods: mlib (default), our-nlp, databricks-nlp"
    opt[Int]("limit") action { (x, c) => c.copy(limit = x) } text "use this number as sample size for detection (and limit/10 is the display count)"
    opt[Unit]('t', "train") action { (_, c) => c.copy(train = true) } text "train model using input file"
    opt[Unit]('v', "verbose") action { (_, c) => c.copy(verbose = true) } text "let's be very chatty (note that setting this will slow down everything)"
    opt[Unit]("vvvv") action { (_, c) => c.copy(very_verbose = true) } text "let's all be very very chatty (note that setting this will severely slow down everything)"

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
        val spark = SparkSession
          .builder
          .appName("getTweetsNativeVSImplicit")
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.shuffle.service.enabled","true")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        import spark.implicits._

        if (config.train) {
          log.info("constructing training data")
          val tset = TrainingSet.getTrainingAndTestingDataFrames(config.inputs, config.inputFileType, spark, config.limit, verbosity)
          if (tset.isEmpty) {
            log.error("could not detect training data")
            sys.exit(-3)
          }
          log.info("cleaning data and paths")
          val training = tset.map(t => Helper.cleanSource(Detection.detectTextColumn(t, config.limit).get, outputColumn, t)).get
          Helper.clean(spark, config.model)
          log.info("training machine learning model")
          val model = MlLibSentimentAnalyser.train(spark, training, outputColumn, config.verbose, config.very_verbose, config.limit)
          model.save(config.model)
        } else {

          if (config.output.isDefined) {
            Helper.clean(spark, config.output.get)
          }

          log.info("loading files from " + config.inputs.foreach(println))
          val sources = config.inputs.map(i => (i, IO.loadFile(config.inputFileType, i, spark)))
          for ((name, source) <- sources) {
            if (source.isEmpty) {
              log.error("could not load source " + name + " unknown/unsupported filetype")
            } else {

              val data = source.get._1
              val dtype = source.get._2
              if (config.very_verbose) {
                log.debug("Got " + data.count() + " lines.")
                display(math.ceil(config.limit/10).toInt, data)
              }

              val column = if (config.column.isEmpty) {
                  Detection.detectTextColumn(data, config.limit)
              } else {
                config.column
              }
              if (column.isEmpty) {
                log.error("could not find a text column to analyse for " + name)
              } else {
                log.info("selected column: " + column)

                val cleaned = Helper.cleanSource(column.get, outputColumn, data)
                if (verbosity) {
                  log.debug("cleaned source data:")
                  cleaned.show(math.ceil(config.limit/10).toInt)
                }

                if (config.method.isEmpty || config.method.get.toLowerCase == "mlib") {
                  log.info("loading MLib model")
                  val model = MlLibSentimentAnalyser.load(config.model)
                  log.info("select sentiment data using MLib")
                  val output = model.transform(cleaned)
                  process(config, output.toDF, dtype)
                } else if (config.method.get.toLowerCase == "our-nlp") {
                  log.info("select sentiment data using our CoreNLP method")
                  val output = cleaned.map(f => (f.getAs[Seq[String]](outputColumn), new CoreNLPSentimentAnalyzer(verbosity).computeSentiment(f.getAs[Seq[String]](outputColumn).mkString(" "))))
                  process(config, output.toDF, dtype)
                } else {
                  log.info("select sentiment data CoreNLP databricks")
                  val output = cleaned.withColumn("sentiment", sentiment(Symbol(outputColumn)))
                  process(config, output, dtype)
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

  private def display(count: Int, data: DataFrame) = {
    if (count == 0) {
      log.info("displaying contents:")
      log.info(data.collect.foreach(println))
    } else {
      log.info("displaying first " + count + " contents:")
      log.info(data.take(count).foreach(println))
    }
  }

  private def process(config: Config, output: DataFrame, dtype: String) = {
    val data = if (!config.very_verbose) {
      output.drop("filtered").drop("words").drop("tf").drop("tfidf").drop("rawPrediction").drop("probability")
    } else output
    display(math.ceil(config.limit/10).toInt, data)
    if (config.output.isDefined) {
      log.info("saving sentiments")
      IO.save(dtype, data, config.output.get)
    }
  }

  case class Config(inputs: Seq[String] = Seq(),
                    model: String = "sentiment.model",
                    inputFileType: Option[String] = None,
                    output: Option[String] = None,
                    column: Option[String] = None,
                    method: Option[String] = None,
                    limit: Int = 100,
                    train: Boolean = false,
                    verbose: Boolean = false,
                    very_verbose: Boolean = false)

}