package ch.uzh.sentiment.utils

import org.scalatest._

class EmojiTests extends FlatSpec with ShouldMatchers {

  behavior of "emoji"

  it should "get positive value" in {
    val words = Map(
      ":)" -> "happy",
      ":D" -> "happy"
    )

    for ((word, result) <- words)
      assertResult(result)(new Emoji(null).get(word))
  }

  it should "get negative value" in {
    val words = Map(
      ":(" -> "sad",
      ":-(" -> "sad"
    )

    for ((word, result) <- words)
      assertResult(result)(new Emoji(null).get(word))
  }

}
