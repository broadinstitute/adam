/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.converters

import org.scalatest._
import GVCFLineConverter._
import edu.berkeley.cs.amplab.adam.avro.GBGenotype

class GVCFLineConverterSuite extends FunSuite {

  test("plIndices returns correct indices in correct order") {
    assert(plIndices(0) === Seq((0, 0)))
    assert(plIndices(1) === Seq((0, 0), (0, 1), (1, 1)))
    assert(plIndices(2) === Seq((0, 0), (0, 1), (1, 1), (0, 2), (1, 2), (2, 2)))
  }

  test("convert handles a NON_REF line correctly") {
    val line = "20\t62545\t.\tC\t<NON_REF>\t.\t.\tBLOCK_SIZE=5;END=62549\tGT:DP:GQ:MIN_DP:MIN_GQ:PL\t0/0:3:3:3:3:0,3,45"
    val gts = GVCFLineConverter.convert(Array("NA12878"), line)

    assert(gts.length === 3)

    assert( gts.referenceNames all_equal "20" )
    assert( gts.starts all_equal 62545L )
    assert( gts.referenceAlleles all_equal "C" )
    assert( gts.lengths all_equal 5 )
  }

  class GenotypeSeq(itr : Iterable[GBGenotype]) {
    def referenceNames = itr.map(_.getReferenceName)
    def starts = itr.map(_.getStart)
    def referenceAlleles = itr.map(_.getReferenceAllele)
    def lengths = itr.map(_.getLength)
  }

  class CheckableIterable[T](itr : Iterable[T]) {
    def all_equal(target : T) : Boolean = !itr.exists(_ != target)
  }

  implicit def convertIterableToCheckable[T](itr : Iterable[T]) : CheckableIterable[T] =
    new CheckableIterable[T](itr)

  implicit def convertGBGenotypesToGenotypeSeq(itr : Iterable[GBGenotype]) : GenotypeSeq =
    new GenotypeSeq(itr)
}
