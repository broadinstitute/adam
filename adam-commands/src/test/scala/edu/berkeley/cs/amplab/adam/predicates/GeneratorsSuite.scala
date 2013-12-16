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

import edu.berkeley.cs.amplab.adam.util.{Histogram, Points, SparkFunSuite}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket

class GeneratorsSuite extends SparkFunSuite {
  var bucket: SingleReadBucket = null
  var bucketMapq: SingleReadBucket = null
  var bucketDuplicate: SingleReadBucket = null
  var bucketQual: SingleReadBucket = null
  var bucketMovedChromosome: SingleReadBucket = null
  var bucketMovedStart: SingleReadBucket = null

  sparkBefore("Generators setup") {
    def srb(record: ADAMRecord): SingleReadBucket = {
      val seq = Seq(record)
      val rdd = sc.makeRDD(seq)
      val srbRDD = SingleReadBucket(rdd)
      srbRDD.first()
    }

    val record: ADAMRecord = ADAMRecord.newBuilder()
      .setReadName("test")
      .setDuplicateRead(false)
      .setMapq(10)
      .setQual("abcdef")
      .setReferenceId(1)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setRecordGroupId("groupid")
      .setReadMapped(true)
      .build()

    bucket = srb(record)

    bucketMapq = srb(ADAMRecord.newBuilder(record)
      .setMapq(11)
      .build())

    bucketDuplicate = srb(ADAMRecord.newBuilder(record)
      .setDuplicateRead(true)
      .build())

    bucketQual = srb(ADAMRecord.newBuilder(record)
      .setQual("fedcba")
      .build())

    bucketMovedChromosome = srb(ADAMRecord.newBuilder(record)
      .setReferenceId(2)
      .setStart(200)
      .build())

    bucketMovedStart = srb(ADAMRecord.newBuilder(record)
      .setStart(200)
      .build())
  }

  sparkTest("Dupe mismatches found") {
    assert(GBGenerators.DupeMismatch.matchedByName(bucket, bucket).asInstanceOf[Int] === 0)
    assert(GBGenerators.DupeMismatch.matchedByName(bucket, bucketDuplicate).asInstanceOf[Int] === 1)
  }

  sparkTest("Mismatched mapped positions histogram generated") {
    assert(GBGenerators.MappedPosition.matchedByName(bucket, bucket).asInstanceOf[Histogram[Long]].countToValue(0) === 1)
    assert(GBGenerators.MappedPosition.matchedByName(bucket, bucketMovedChromosome).asInstanceOf[Histogram[Long]].countToValue.get(0).isEmpty)
    assert(GBGenerators.MappedPosition.matchedByName(bucket, bucketMovedChromosome).asInstanceOf[Histogram[Long]].countToValue(Long.MaxValue) === 1)
    assert(GBGenerators.MappedPosition.matchedByName(bucket, bucketMovedStart).asInstanceOf[Histogram[Long]].countToValue.get(0).isEmpty)
    assert(GBGenerators.MappedPosition.matchedByName(bucket, bucketMovedStart).asInstanceOf[Histogram[Long]].countToValue(100) === 1)
  }

  sparkTest("Test map quality scores") {
    assert(GBGenerators.MapQualityScores.matchedByName(bucket, bucket).asInstanceOf[Points[Int]].points.contains((10, 10)))
    assert({
      val points = GBGenerators.MapQualityScores.matchedByName(bucket, bucketMapq).asInstanceOf[Points[Int]].points
      points.contains((10, 11)) || points.contains((11, 10))
    })
  }
}
