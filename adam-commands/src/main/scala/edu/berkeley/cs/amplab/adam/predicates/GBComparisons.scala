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
import edu.berkeley.cs.amplab.adam.projections.FieldValue
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._
import edu.berkeley.cs.amplab.adam.util.Points
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object GBComparisons {
  private[predicates] object DupeMismatch extends BooleanBucketComparisons with Serializable {
    val name = "dupes"
    val description = "Counts the number of common reads marked as duplicates"

    def matches(records1: Seq[ADAMRecord], records2: Seq[ADAMRecord]): Boolean =
      records1.size == records2.size && (records1.size match {
        case 0 => true
        case 1 => records1.head.getDuplicateRead != records2.head.getDuplicateRead
        case _ => false
      })

    def boolMatchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Boolean =
      matches(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) &&
      matches(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) &&
      matches(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) &&
      matches(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) &&
      matches(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)

    def schemas: Seq[FieldValue] = Seq(duplicateRead)
  }

  private[predicates] object MappedPosition extends DistanceBucketComparisons with Serializable {
    val name = "positions"
    val description = "Counts how many reads align to the same genomic location"

    def distance(records1: Seq[ADAMRecord], records2: Seq[ADAMRecord]): Long = {
      if (records1.size == records2.size) records1.size match {
        case 0 => 0
        case 1 => {
          val r1 = records1.head
          val r2 = records2.head
          if (r1.getReferenceId == r2.getReferenceId) {
            val start1 = r1.getStart
            val start2 = r2.getStart
            if (start1 > start2) start1 - start2 else start2 - start1
          } else -1
        }
        case _ => -1
      }
      else -1
    }

    /**
     * The records have been matched by their names, but the rest may be mismatched.
     */
    def longMatchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Long =
      distance(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) +
        distance(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) +
        distance(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) +
        distance(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) +
        distance(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)


    def schemas: Seq[FieldValue] = Seq(
      referenceId,
      start,
      firstOfPair)
  }

  private[predicates] object MapQualityScores extends PointsBucketComparisons[Int] with Serializable {
    val name = "mapqs"
    val description = "Creates scatter plot of mapping quality scores across identical reads"

    def points(records1: Seq[ADAMRecord], records2: Seq[ADAMRecord]): Option[(Int, Int)] = {
      if (records1.size == records2.size) {
        records1.size match {
          case 0 => None
          case 1 => Some((records1.head.getMapq, records2.head.getMapq))
          case _ => None
        }
      } else None
    }

    def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any =
      Points(Seq(
        points(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads),
          points(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads),
          points(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads),
          points(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads),
          points(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)).flatten: _*)

    def schemas: Seq[FieldValue] = Seq(mapq)
  }

  private[predicates] object BaseQualityScores extends PointsBucketComparisons[Int] with Serializable {
    val name = "baseqs"
    val description = "Creates scatter plots of base quality scores across identical positions in the same reads"

    def points(records1: Seq[ADAMRecord], records2: Seq[ADAMRecord]): Points[Int] = {
      if (records1.size == records2.size) {
        records1.size match {
          case 0 => Points()
          case 1 => {
            val record1 = records1.head
            val record2 = records2.head
            Points(record1.qualityScores
              .zip(record2.qualityScores)
              .map(b => (b._1.toInt, b._2.toInt)): _*)
          }
          case _ => Points()
        }
      } else Points()
    }

    def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any =
        points(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) ++
        points(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) ++
        points(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) ++
        points(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) ++
        points(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)

    def schemas: Seq[FieldValue] = Seq(qual)
  }

  val generators : Seq[BucketComparisons] = Seq[BucketComparisons](
    DupeMismatch,
    MappedPosition,
    MapQualityScores,
    BaseQualityScores)
}
