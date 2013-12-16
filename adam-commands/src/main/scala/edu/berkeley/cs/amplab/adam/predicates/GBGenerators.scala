/*
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.predicates

import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.util.Points
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object GBGenerators {
  private[predicates] object DupeMismatch extends BooleanGenerator with Serializable {
    val name = "dupes"

    def boolMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Boolean =
      bucket1.primaryMapped.size != bucket2.primaryMapped.size ||
        (bucket1.primaryMapped.size == 1 &&
          bucket1.primaryMapped.head.getDuplicateRead != bucket2.primaryMapped.head.getDuplicateRead)
  }

  class SameMappedPosition(map : Map[Int,Int]) extends BooleanGenerator with Serializable {
    val name = "same_positions"

    def samePairLocation(map : Map[Int,Int], read1 : ADAMRecord, read2: ADAMRecord) : Boolean =
      sameLocation(map, read1, read2) && sameMateLocation(map, read1, read2)

    def sameLocation(map : Map[Int,Int], read1 : ADAMRecord, read2 : ADAMRecord) : Boolean = {
      assert(map.contains(read1.getReferenceId),
        "referenceId %d of read %s not in map %s".format(read1.getReferenceId.toInt, read1.getReadName, map))

      map(read1.getReferenceId) == read2.getReferenceId.toInt &&
        read1.getStart == read2.getStart && read1.getReadNegativeStrand == read2.getReadNegativeStrand
    }

    def sameMateLocation(map : Map[Int,Int], read1 : ADAMRecord, read2 : ADAMRecord) : Boolean = {

      assert(map.contains(read1.getMateReferenceId),
        "mateReferenceId %d of read %s not in map %s".format(read1.getMateReferenceId.toInt, read1.getReadName, map))

      map(read1.getMateReferenceId) == read2.getMateReferenceId.toInt &&
        read1.getMateAlignmentStart == read2.getMateAlignmentStart &&
        read1.getMateNegativeStrand == read2.getMateNegativeStrand
    }

    def readLocationsMatchPredicate(bucket1 : SingleReadBucket, bucket2 : SingleReadBucket) : Boolean = {

      val (paired1, single1) = bucket1.primaryMapped.partition(_.getReadPaired)
      val (paired2, single2) = bucket2.primaryMapped.partition(_.getReadPaired)

      if(single1.size != single2.size) return false
      if(single1.size == 1 && !sameLocation(map, single1.head, single2.head)) return false

      // TODO: if there are multiple primary hits for single-ended reads?

      val (firstPairs1, _) = paired1.partition(_.getFirstOfPair)
      val (firstPairs2, _) = paired2.partition(_.getFirstOfPair)

      if(firstPairs1.size != firstPairs2.size) return false
      if(firstPairs1.size == 1 && !samePairLocation(map, firstPairs1.head, firstPairs2.head)) return false

      // TODO: if there are multiple primary hits for paired-end reads?

      true
    }

    def boolMatchedByName(bucket1 : SingleReadBucket, bucket2 : SingleReadBucket) : Boolean =
      readLocationsMatchPredicate(bucket1, bucket2)
  }

  private[predicates] object MappedPosition extends DistanceGenerator with Serializable {
    val name = "positions"

    /**
     * The records have been matched by their names, but the rest may be mismatched.
     */
    def longMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Long =
      if (bucket1.primaryMapped.size == bucket2.primaryMapped.size && bucket1.primaryMapped.size == 1) {
        val record1 = bucket1.primaryMapped.head
        val record2 = bucket2.primaryMapped.head
        if (record1.getReferenceId == record2.getReferenceId)
          if (record1.getStart > record2.getStart) record1.getStart - record2.getStart
          else record2.getStart - record1.getStart
        else Long.MaxValue
      } else Long.MaxValue
  }

  private[predicates] object MapQualityScores extends PointGenerator[Int] with Serializable {
    val name = "mapqs"

    def pointMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): (Int, Int) =
      if (bucket1.primaryMapped.size == bucket2.primaryMapped.size && bucket1.primaryMapped.size == 1) {
        val record1 = bucket1.primaryMapped.head
        val record2 = bucket2.primaryMapped.head
        (record1.getMapq, record2.getMapq)
      } else (0, 0)
  }

  private[predicates] object BaseQualityScores extends PointsGenerator[Int] with Serializable {
    val name = "baseqs"

    def matchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Points[Int] =
      if (bucket1.primaryMapped.size == bucket2.primaryMapped.size && bucket1.primaryMapped.size == 1) {
        val record1 = bucket1.primaryMapped.head
        val record2 = bucket2.primaryMapped.head
        record1.qualityScores
          .zip(record2.qualityScores)
          .map(b => Points[Int]((b._1.toInt, b._2.toInt)))
          .fold(Points[Int]())((p1, p2) => p1 ++ p2)
      } else Points[Int]()
  }

  val generators : Seq[MatchGenerator] = Seq[MatchGenerator](
    DupeMismatch,
    MappedPosition,
    MapQualityScores,
    BaseQualityScores)
}
