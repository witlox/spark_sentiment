package ch.uzh.sentiment.utils

import org.scalatest._

class WordListTests extends FlatSpec with ShouldMatchers {

  behavior of "wordlist"

  val wl = new WordList()

  it should "get positive value" in {
    val words = Map(
      "nice" -> 1,
      "happy" -> 1
    )

    for ((word, result) <- words)
      assertResult(result)(wl.value(word))
  }

  it should "get negative value" in {
    val words = Map(
      "bad" -> -1,
      "badly" -> -1
    )

    for ((word, result) <- words)
      assertResult(result)(wl.value(word))
  }

}
