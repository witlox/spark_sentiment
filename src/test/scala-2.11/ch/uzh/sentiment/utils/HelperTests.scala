package ch.uzh.sentiment.utils

import org.scalatest._

class HelperTests extends FunSuite with PrivateMethodTester {

  test("stringDistance should work on empty strings") {
    assert( Helper.stringDistance(   "",    "") == 0 )
    assert( Helper.stringDistance(  "a",    "") == 1 )
    assert( Helper.stringDistance(   "",   "a") == 1 )
    assert( Helper.stringDistance("abc",    "") == 3 )
    assert( Helper.stringDistance(   "", "abc") == 3 )
  }

  test("stringDistance should work on equal strings") {
    assert( Helper.stringDistance(   "",    "") == 0 )
    assert( Helper.stringDistance(  "a",   "a") == 0 )
    assert( Helper.stringDistance("abc", "abc") == 0 )
  }

  test("stringDistance should work where only inserts are needed") {
    assert( Helper.stringDistance(   "",   "a") == 1 )
    assert( Helper.stringDistance(  "a",  "ab") == 1 )
    assert( Helper.stringDistance(  "b",  "ab") == 1 )
    assert( Helper.stringDistance( "ac", "abc") == 1 )
    assert( Helper.stringDistance("abcdefg", "xabxcdxxefxgx") == 6 )
  }

  test("stringDistance should work where only deletes are needed") {
    assert( Helper.stringDistance(  "a",    "") == 1 )
    assert( Helper.stringDistance( "ab",   "a") == 1 )
    assert( Helper.stringDistance( "ab",   "b") == 1 )
    assert( Helper.stringDistance("abc",  "ac") == 1 )
    assert( Helper.stringDistance("xabxcdxxefxgx", "abcdefg") == 6 )
  }

  test("stringDistance should work where only substitutions are needed") {
    assert( Helper.stringDistance(  "a",   "b") == 1 )
    assert( Helper.stringDistance( "ab",  "ac") == 1 )
    assert( Helper.stringDistance( "ac",  "bc") == 1 )
    assert( Helper.stringDistance("abc", "axc") == 1 )
    assert( Helper.stringDistance("xabxcdxxefxgx", "1ab2cd34ef5g6") == 6 )
  }

  test("stringDistance should work where many operations are needed") {
    assert( Helper.stringDistance("example", "samples") == 3 )
    assert( Helper.stringDistance("sturgeon", "urgently") == 6 )
    assert( Helper.stringDistance("distance", "difference") == 5 )
  }

  test("standard distance between regular dictionary and tweet grammer") {
    assert( Helper.stringDistance("noooo", "no") == 3 )
    assert( Helper.stringDistance("great", "gr8t") == 2 )
    assert( Helper.stringDistance("n00b", "noob") == 2 )
  }

}
