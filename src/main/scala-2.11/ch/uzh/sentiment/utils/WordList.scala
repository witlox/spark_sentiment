package ch.uzh.sentiment.utils

class WordList extends Serializable {

  /***
    * Get a list of words with there corresponding sentiment score, positive = 1, negative = -1
    * @return Map of [word, value]
    */
  lazy val getAll: Map[String, Double] = {
    val pos = IO.loadWordPositiveWordListResource.map(_.toLowerCase).map(s => Stemmer.stem(s) -> 1.0)
    val neg = IO.loadWordNegativeWordListResource.map(_.toLowerCase).map(s => Stemmer.stem(s) -> -1.0)
    (pos ++ neg).groupBy(_._1).map(e => e._1 -> (e._2.map(_._2).sum / e._2.length))
  }

  def positive: List[String] = IO.loadWordPositiveWordListResource.map(_.toLowerCase)

  def negative: List[String] = IO.loadWordNegativeWordListResource.map(_.toLowerCase)

  private def dx(word: String): Double = {
    def df(word: String, lim: Int): Double = {
      val sub = getAll.filter(x => Helper.stringDistance(x._1, word) < lim)
      sub.values.sum / sub.size
    }
    if (getAll.exists(w => Helper.stringDistance(w._1, word) > 3)) {
      0.0
    } else if (getAll.exists(w => Helper.stringDistance(w._1, word) < 1)) {
      df(word, 1)
    } else if (getAll.exists(w => Helper.stringDistance(w._1, word) < 2)) {
      df(word, 2)
    } else {
      df(word, 3)
    }
  }

  private def evaluate(word: String) = {
    if (getAll.get(word).isDefined) {
      getAll(word)
    } else if (getAll.get(word.distinct).isDefined) {
      getAll(word.distinct)
    } else {
      if (math.abs(dx(word)) > math.abs(dx(word.distinct))) {
        dx(word)
      } else {
        dx(word.distinct)
      }
    }
  }

  def value(word: String): Double = {
    val stem = Stemmer.stem(word)
    if (stem == word) {
      evaluate(word)
    } else {
      val we = evaluate(word)
      val se = evaluate(stem)
      if (math.abs(we) > math.abs(se)) {
        we
      } else {
        se
      }
    }
  }
}
