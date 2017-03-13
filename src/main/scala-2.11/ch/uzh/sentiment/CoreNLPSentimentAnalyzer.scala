package ch.uzh.sentiment

import java.util.Properties

import ch.uzh.sentiment.utils.Timing
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.log4j.{LogManager, Logger}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class CoreNLPSentimentAnalyzer(timing: Boolean) {

  val log: Logger = LogManager.getLogger(getClass.getName)
  val timer: Timing = new Timing(timing)

  class StanfordCoreNLPWrapper(private val props: Properties) extends Serializable {

    @transient private var coreNLP: StanfordCoreNLP = _

    def get: StanfordCoreNLP = {
      if (coreNLP == null) {
        log.debug("creating NLP instance")
        coreNLP = new StanfordCoreNLP(props)
      }
      coreNLP
    }

  }

  def truncate(value: String,length: Int) : String = {
    var return_val = value
    if (value != null && value.length() > length) {
      return_val = value.substring(0, length)
    }
    return_val
  }

  lazy val pipeline = {
    log.debug("setting up new pipeline")
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    new StanfordCoreNLPWrapper(props)
  }

  def computeSentiment(text: String): Int = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = timer.time("process annotations", { pipeline.get.process(text) })
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, RNNCoreAnnotations.getPredictedClass(tree)) }
      .toList
  }

  def computeWeightedSentiment(tweet: String): Int = {
    val annotation = timer.time("extracting annotations", { pipeline.get.process(tweet) })
    val sentiments: ListBuffer[Double] = ListBuffer()
    val sizes: ListBuffer[Int] = ListBuffer()

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)

      sentiments += sentiment.toDouble
      sizes += sentence.toString.length
    }

    val weightedSentiment = if (sentiments.isEmpty) {
      -1
    } else {
      val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
      weightedSentiments.sum / sizes.sum
    }

    weightedSentiment.toInt
  }

}

object CoreNLPSentimentAnalyzer {
  def apply(timing: Boolean): CoreNLPSentimentAnalyzer = new CoreNLPSentimentAnalyzer(timing)

}