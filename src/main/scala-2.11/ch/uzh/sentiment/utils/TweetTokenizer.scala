/**
* TweetMotif is licensed under the Apache License 2.0:
* http://www.apache.org/licenses/LICENSE-2.0.html
* Copyright Brendan O'Connor, Michel Krieger, and David Ahn, 2009-2010.
* Adapted for use within the sentiment tester
*/

package ch.uzh.sentiment.utils

import scala.util.matching.Regex

object TweetTokenizer {

  val contraction: Regex = """(?i)(\w+)(n't|'ve|'ll|'d|'re|'s|'m)$""".r
  val ws: Regex = """\s+""".r

  val punctuation = """['“\".?!,:;]"""
  val punctuationSeq: String = punctuation + """+"""
  val entity = """&(amp|lt|gt|quot);"""

  val urlStart1 = """(https?://|www\.)"""
  val commonTLDs = """(com|co\.uk|org|net|info|ca|ly|mp|edu|gov)"""
  val urlStart2: String = """[A-Za-z0-9\.-]+?\.""" + commonTLDs + """(?=[/ \W])"""
  val urlBody = """[^ \t\r\n<>]*?"""
  val urlExtraCrapBeforeEnd: String = "(" + punctuation + "|" + entity + ")+?"
  val urlEnd = """(\.\.+|[<>]|\s|$)"""
  val url: String = """\b(""" + urlStart1 + "|" + urlStart2 + ")" + urlBody + "(?=(" + urlExtraCrapBeforeEnd + ")?" + urlEnd + ")"

  val timeLike = """\d+:\d+"""
  val numNum = """\d+\.\d+"""
  val numberWithCommas: String = """(\d+,)+?\d{3}""" + """(?=([^,]|$))"""

  val boundaryNotDot: String = """($|\s|[“\"?!,:;]|""" + entity + ")"
  val aa1: String = """([A-Za-z]\.){2,}(?=""" + boundaryNotDot + ")"
  val aa2: String = """[^A-Za-z]([A-Za-z]\.){1,}[A-Za-z](?=""" + boundaryNotDot + ")"
  val standardAbbreviations = """\b([Mm]r|[Mm]rs|[Mm]s|[Dd]r|[Ss]r|[Jj]r|[Rr]ep|[Ss]en|[Ss]t)\."""
  val arbitraryAbbrev: String = "(" + aa1 + "|" + aa2 + "|" + standardAbbreviations + ")"

  val separators = "(--+|―)"
  val decorations = """[♫]+"""
  val thingsThatSplitWords = """[^\s\.,]"""
  val embeddedApostrophe: String = thingsThatSplitWords + """+'""" + thingsThatSplitWords + """+"""

  val normalEyes = "(?iu)[:=]"
  val wink = "[;]"
  val noseArea = "(|o|O|-|[^a-zA-Z0-9 ])"
  val happyMouths = """[D\)\]]+"""
  val sadMouths = """[\(\[]+"""
  val tongue = "[pP]"
  val otherMouths = """[doO/\\]+""" // remove forward slash if http://'s aren't cleaned

  def OR(parts: String*): String = "(" + parts.toList.mkString("|") + ")"

  val emoticon = OR(OR(normalEyes, wink) + noseArea + OR(tongue, otherMouths, sadMouths, happyMouths),
    """(?<=( |^))""" + OR(sadMouths, happyMouths, otherMouths) + noseArea + OR(normalEyes, wink)
  )

  def allowEntities(pat: String): String = pat.replace("<", "(<|&lt;)").replace(">", "(>|&gt;)")
  val Hearts: String = allowEntities("""(<+/?3+)""")
  val Arrows: String = allowEntities("""(<*[-=]*>+|<+[-=]*>*)""")
  val Hashtag = """#[a-zA-Z0-9_]+"""
  val AtMention = """@[a-zA-Z0-9_]+"""
  val Bound = """(\W|^|$)"""
  val Email: String = "(?<=" + Bound + """)[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}(?=""" + Bound + ")"

  val Protected = new Regex(OR(Hearts, Arrows, emoticon, url, Email, entity, timeLike, numNum, numberWithCommas,
      punctuationSeq, arbitraryAbbrev, separators, decorations, embeddedApostrophe, Hashtag, AtMention))

  val edgePunctuationChars = """'"“”‘’«»{}\(\)\[\]\*"""
  val edgePunctuated: String = "[" + edgePunctuationChars + "]"
  val notEdgePunctuated = "[a-zA-Z0-9]"
  val offEdge = """(^|$|:|;|\s)"""
  val EdgePunctuatedLeft = new Regex(offEdge + "(" + edgePunctuated + "+)(" + notEdgePunctuated + ")")
  val EdgePunctuatedRight = new Regex("(" + notEdgePunctuated + ")(" + edgePunctuated + "+)" + offEdge)

  def splitEdgePunctuation(input: String): String = {
    var s = input
    s = EdgePunctuatedLeft.replaceAllIn(s, "$1$2 $3")
    s = EdgePunctuatedRight.replaceAllIn(s, "$1 $2$3")
    s
  }

  def simpleTokenize(text: String): List[String] = {

    val splitPunctuationText = splitEdgePunctuation(text)

    val matches = Protected.findAllIn(splitPunctuationText).matchData.toList

    val badSpans = matches map (mat => Tuple2(mat.start, mat.end))
    val indices = (0 :: badSpans.foldRight(List[Int]())((x, y) => x._1 :: x._2 :: y)) ::: List(splitPunctuationText.length)

    val goods = indices.grouped(2).map { x => splitPunctuationText.slice(x.head, x(1)) }.toList
    val splitGoods = goods map { str => str.trim.split(" ").toList }

    val bads = badSpans map { case (start, end) => List(splitPunctuationText.slice(start, end)) }

    val zippedStr = (if (splitGoods.length == bads.length) {
      splitGoods.zip(bads) map { pair => pair._1 ++ pair._2 }
    } else {
      (splitGoods.zip(bads) map { pair => pair._1 ++ pair._2 }) ::: List(splitGoods.last)
    }).flatten

    zippedStr.flatMap(splitToken).filter(_.length > 0)
  }

  def squeezeWhitespace(input: String): String = ws.replaceAllIn(input, " ").trim

  def splitToken(token: String): List[String] = {
    token match {
      case `token` => List(token.trim)
    }
  }

  def apply(text: String): List[String] = simpleTokenize(squeezeWhitespace(text))

  def tokenize(text: String): List[String] = apply(text)

  def normalizeText(text: String): String = text.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&")

  def tokenizeForTagger(text: String): List[String] = tokenize(text).map(normalizeText)

  def tokenizeToString(text: String): String = tokenizeForTagger(text).mkString(" ")

}
