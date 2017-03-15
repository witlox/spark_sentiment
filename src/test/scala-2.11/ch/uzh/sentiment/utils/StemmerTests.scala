package ch.uzh.sentiment.utils

import org.scalatest._

class StemmerTests extends FlatSpec with ShouldMatchers {

  behavior of "stemmer"

  it should "stem plurals" in {
    val plurals = Map(
      "caresses" -> "caress",
      "caress" -> "caress",
      "tonies" -> "toni",
      "pies" -> "pi",
      "dogs" -> "dog"
    )

    for ((word, stem) <- plurals)
      assertResult(stem)(Stemmer.stem(word))
  }

  it should "stem past participles" in {
    val participles = Map(
      "feed" -> "fe",
      "agreed" -> "agr",
      "plastered" -> "plaster",
      "bled" -> "bled",
      "motoring" -> "motor",
      "hissing" -> "hiss",
      "fizzed" -> "fizz",
      "failing" -> "fail",
      "filing" -> "file",
      "happy" -> "happi",
      "sky" -> "sky"
    )

    for ((word, stem) <- participles) {
      assertResult(stem)(Stemmer.stem(word))
    }
  }

  it should "change suffixes" in {
    val changes = Map(
      "relational" -> "relat",
      "conditional" -> "condit",
      "rational" -> "ration",
      "valenci" -> "valenc",
      "hesitanci" -> "hesit",
      "digitizer" -> "digit",
      "conformabli" -> "conform",
      "radicalli" -> "radic",
      "differentli" -> "differ",
      "vileli" -> "vile",
      "analogousli" -> "analog",
      "vietnamization" -> "vietnam",
      "predication" -> "predic",
      "operator" -> "oper",
      "hopeful" -> "hope",
      "goodness" -> "good",
      "revival" -> "reviv",
      "allowance" -> "allow",
      "inference" -> "infer",
      "airliner" -> "airlin",
      "gyroscopic" -> "gyroscop",
      "adjustable" -> "adjust",
      "defensible" -> "defens",
      "irritant" -> "irrit",
      "replacement" -> "replac",
      "adjustment" -> "adjust",
      "dependent" -> "depend",
      "adoption" -> "adopt",
      "homologou" -> "homolog",
      "communism" -> "commun",
      "activate" -> "activ",
      "angulariti" -> "angular",
      "homologous" -> "homolog",
      "effective" -> "effect",
      "roll" -> "roll"
    )

    for ((word, stem) <- changes) {
      assertResult(stem)(Stemmer.stem(word))
    }
  }
}
