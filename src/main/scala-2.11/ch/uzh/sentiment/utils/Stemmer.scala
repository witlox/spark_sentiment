package ch.uzh.sentiment.utils

/**
* See http://snowball.tartarus.org/algorithms/porter/stemmer.html
**/
object Stemmer {

  def stem(word: String): String = {

    var stem = new Word(word).applyReplaces(
      "sses" -> "ss",
      "ies" -> "i",
      "ss" -> "ss",
      "s" -> "")

    if ((stem matchedBy ((~v~) + "ed")) || (stem matchedBy ((~v~) + "ing"))) {
      stem = stem.applyReplaces(~v~)("ed" -> "", "ing" -> "")
      stem = stem.applyReplaces(
        "at" -> "ate",
        "bl" -> "ble",
        "iz" -> "ize",
        (~d and not(~L or ~S or ~Z)) -> singleLetter,
        (m == 1 and ~o) -> "e")
    } else {
      stem = stem.applyReplaces(((m > 0) + "eed") -> "ee")
    }

    stem = stem.applyReplaces(((~v~) + "y") -> "i")

    stem = stem.applyReplaces(m > 0)(
      "ational" -> "ate",
      "tional" -> "tion",
      "enci" -> "ence",
      "anci" -> "ance",
      "izer" -> "ize",
      "abli" -> "able",
      "alli" -> "al",
      "entli" -> "ent",
      "eli" -> "e",
      "ousli" -> "ous",
      "ization" -> "ize",
      "ation" -> "ate",
      "ator" -> "ate",
      "alism" -> "al",
      "iveness" -> "ive",
      "fulness" -> "ful",
      "ousness" -> "ous",
      "aliti" -> "al",
      "iviti" -> "ive",
      "biliti" -> "ble"
    )

    stem = stem.applyReplaces(m > 0)(
      "icate" -> "ic",
      "ative" -> "",
      "alize" -> "al",
      "iciti" -> "ic",
      "ical" -> "ic",
      "ful" -> "",
      "ness" -> ""
    )

    stem = stem.applyReplaces(m > 1)(
      "al" -> "",
      "ance" -> "",
      "ence" -> "",
      "er" -> "",
      "ic" -> "",
      "able" -> "",
      "ible" -> "",
      "ant" -> "",
      "ement" -> "",
      "ment" -> "",
      "ent" -> "",
      ((~S or ~T) + "ion") -> "",
      "ou" -> "",
      "ism" -> "",
      "ate" -> "",
      "iti" -> "",
      "ous" -> "",
      "ive" -> "",
      "ize" -> ""
    )

    stem = stem applyReplaces(((m > 1) + "e") -> "", (((m == 1) and not(~o)) + "e") -> "")

    stem = stem applyReplaces ((m > 1 and ~d and ~L) -> singleLetter)

    stem.toString
  }

  private case class Pattern(condition: Condition, suffix: String)

  private case class Condition(predicate: Word => Boolean) {
    def + = Pattern(this, _: String)

    def unary_~ : Condition = this

    def ~ : Condition = this

    def and(condition: Condition) = Condition((word) => predicate(word) && condition.predicate(word))

    def or(condition: Condition) = Condition((word) => predicate(word) || condition.predicate(word))
  }

  private def not: Condition => Condition = {
    case Condition(predicate) => Condition(!predicate(_))
  }

  private val emptyCondition = Condition(_ => true)

  private object m {
    def >(measure: Int) = Condition(_.measure > measure)

    def ==(measure: Int) = Condition(_.measure == measure)
  }

  private val S = Condition(_ endsWith "s")
  private val Z = Condition(_ endsWith "z")
  private val L = Condition(_ endsWith "l")
  private val T = Condition(_ endsWith "t")

  private val d = Condition(_.endsWithCC)

  private val o = Condition(_.endsWithCVC)

  private val v = Condition(_.containsVowels)

  private case class StemBuilder(build: Word => Word)

  private def suffixStemBuilder(suffix: String) = StemBuilder(_ + suffix)

  private val singleLetter = StemBuilder(_ trimSuffix 1)

  private class Word(string: String) {
    val word: String = string.toLowerCase

    def trimSuffix(suffixLength: Int) = new Word(word substring (0, word.length - suffixLength))

    def endsWith: (String) => Boolean = word endsWith

    def +(suffix: String) = new Word(word + suffix)

    def satisfies: (Condition) => Boolean = (_: Condition).predicate(this)

    def hasConsonantAt(position: Int): Boolean =
      (word.indices contains position) && (word(position) match {
        case 'a' | 'e' | 'i' | 'o' | 'u' => false
        case 'y' if hasConsonantAt(position - 1) => false
        case _ => true
      })

    def hasVowelAt: (Int) => Boolean = !hasConsonantAt(_: Int)

    def containsVowels: Boolean = word.indices exists hasVowelAt

    def endsWithCC: Boolean = (word.length > 1) && (word(word.length - 1) == word(word.length - 2)) && hasConsonantAt(word.length - 1)

    def endsWithCVC: Boolean = (word.length > 2) && hasConsonantAt(word.length - 1) && hasVowelAt(word.length - 2) && hasConsonantAt(word.length - 3) && !(Set('w', 'x', 'y') contains word(word.length - 2))

    def measure: Int = word.indices.count(pos => hasVowelAt(pos) && hasConsonantAt(pos + 1))

    def matchedBy: Pattern => Boolean = {
      case Pattern(condition, suffix) => endsWith(suffix) && (trimSuffix(suffix.length) satisfies condition)
    }

    def applyReplaces(replaces: (Pattern, StemBuilder)*): Word = {
      for ((pattern, stemBuilder) <- replaces if matchedBy(pattern))
        return stemBuilder build trimSuffix(pattern.suffix.length)
      this
    }

    def applyReplaces(commonCondition: Condition)(replaces: (Pattern, StemBuilder)*): Word =
      applyReplaces(replaces map {
        case (Pattern(condition, suffix), stemBuilder) => (Pattern(commonCondition and condition, suffix), stemBuilder)
      }: _*)

    override def toString: String = word
  }

  private implicit def ruleExtension[P, SB]
  (rule: (P, SB))(implicit ev$1: P => Pattern, ev$2: SB => StemBuilder): (Pattern, StemBuilder) = (rule._1, rule._2)
  private implicit def emptyConditionPattern: String => Pattern = Pattern(emptyCondition, _)
  private implicit def emptySuffixPattern: Condition => Pattern = Pattern(_, "")
  private implicit def suffixedStemBuilder: String => StemBuilder = suffixStemBuilder
}