/*
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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion, ReferenceMapping}
import edu.berkeley.cs.amplab.adam.rich.ADAMRecordContext._

class ReferenceMappingRDDFunctionsSuite extends SparkFunSuite {
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(SequenceRecord(1, "1", 5, "test://chrom1"))
  }

  test("Single region returns itself") {
    val region = new ReferenceRegion(1, 1, 2)
    val regions = new NonoverlappingRegions(seqDict, Seq(region))
    val result = regions.binaryRegionSearch(region)
    assert(result.size === 1)
    assert(result.head === region)
  }

  test("Nonoverlapping regions will all be returned") {
    val region1 = new ReferenceRegion(1, 1, 2)
    val region2 = new ReferenceRegion(1, 3, 5)
    val testRegion3 = new ReferenceRegion(1, 1, 4)
    val testRegion1 = new ReferenceRegion(1, 4, 5)
    val regions = new NonoverlappingRegions(seqDict, Seq(region1, region2))
    assert(regions.binaryRegionSearch(testRegion3).size === 3)
    assert(regions.binaryRegionSearch(testRegion1).size === 1)
  }

  test("Many overlapping regions will all be merged") {
    val region1 = new ReferenceRegion(1, 1, 3)
    val region2 = new ReferenceRegion(1, 2, 4)
    val region3 = new ReferenceRegion(1, 3, 5)
    val testRegion = new ReferenceRegion(1, 1, 4)
    val regions = new NonoverlappingRegions(seqDict, Seq(region1, region2, region3))
    assert(regions.binaryRegionSearch(testRegion).size === 1)
  }


  test("ADAMRecords return proper references") {
    val built = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = built
    val record2 = ADAMRecord.newBuilder(built).setStart(3).build()
    val baseRecord = ADAMRecord.newBuilder(built).setCigar("4M").build()

    val baseMapping = new NonoverlappingRegions(seqDict, Seq(ADAMRecordReferenceMapping.getReferenceRegion(baseRecord)))
    val regions1 = baseMapping.binaryRegionSearch(ADAMRecordReferenceMapping.getReferenceRegion(record1))
    val regions2 = baseMapping.binaryRegionSearch(ADAMRecordReferenceMapping.getReferenceRegion(record2))
    assert(regions1.size === 1)
    assert(regions2.size === 1)
    assert(regions1.head === regions2.head)
  }

  sparkTest("Ensure same reference regions get passed together") {
    val builder = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")

    val record1 = builder.build()
    val record2 = builder.build()

    val rdd1 = sc.parallelize(Seq(record1))
    val rdd2 = sc.parallelize(Seq(record2))

    assert(ReferenceMappingRDDFunctionsSuite.getReferenceRegion(record1) ===
      ReferenceMappingRDDFunctionsSuite.getReferenceRegion(record2))

    assert(Join.regionJoin[ADAMRecord, ADAMRecord, Boolean](
      sc,
      seqDict,
      rdd1,
      rdd2,
      true,
      ReferenceMappingRDDFunctionsSuite.merge,
      ReferenceMappingRDDFunctionsSuite.and))

    assert(Join.regionJoin[ADAMRecord, ADAMRecord, Int](
      sc,
      seqDict,
      rdd1,
      rdd2,
      0,
      ReferenceMappingRDDFunctionsSuite.count,
      ReferenceMappingRDDFunctionsSuite.sum) === 1)
  }

  sparkTest("Overlapping reference regions") {
    val built = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = built
    val record2 = ADAMRecord.newBuilder(built).setStart(3).build()
    val baseRecord = ADAMRecord.newBuilder(built).setCigar("4M").build()

    val baseRdd = sc.parallelize(Seq(baseRecord))
    val recordsRdd = sc.parallelize(Seq(record1, record2))

    assert(Join.regionJoin[ADAMRecord, ADAMRecord, Boolean](
      sc,
      seqDict,
      baseRdd,
      recordsRdd,
      true,
      ReferenceMappingRDDFunctionsSuite.merge,
      ReferenceMappingRDDFunctionsSuite.and))

    assert(Join.regionJoin[ADAMRecord, ADAMRecord, Int](
      sc,
      seqDict,
      baseRdd,
      recordsRdd,
      0,
      ReferenceMappingRDDFunctionsSuite.count,
      ReferenceMappingRDDFunctionsSuite.sum) === 2)
  }
}

object ReferenceMappingRDDFunctionsSuite {
  def getReferenceRegion[T](record: T)(implicit mapping: ReferenceMapping[T]): ReferenceRegion =
    mapping.getReferenceRegion(record)

  def merge(prev: Boolean, next: (ADAMRecord, ADAMRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}