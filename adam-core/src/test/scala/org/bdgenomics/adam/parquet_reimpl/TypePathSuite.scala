package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.FunSuite

class TypePathSuite extends FunSuite {
  test("Two tail-less type pathes equal") {
    assert(new TypePath("head") === new TypePath("head"))
  }

  test("Two identically tailed type pathes equal") {
    val tail = new TypePath("tail")
    assert(new TypePath("head", tail) === new TypePath("head", tail))
  }

  test("Two type pathes equal with separately constructed tails") {
    assert(new TypePath("head", new TypePath("tail")) === new TypePath("head", new TypePath("tail")))
  }
}
