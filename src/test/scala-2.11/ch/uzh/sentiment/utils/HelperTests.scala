package ch.uzh.sentiment.utils

import org.scalatest._

class HelperTests extends FlatSpec with Matchers {

  "cleanString" should "return a nice and tidy string" in {
    val s = Helper.cleanString("\n\n\"\t #Test  \"   @these strings are http://supposed.to.be    clean :-) !!!!!  ")
    s should be(" test strings are clean :-) !!!!! ")
  }

}
