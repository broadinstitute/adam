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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket
import edu.berkeley.cs.amplab.adam.projections.FieldValue
import edu.berkeley.cs.amplab.adam.serialization.AvroSerializer
import edu.berkeley.cs.amplab.adam.util.{Histogram, Points}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import java.io.Writer

case class ReadBucket(unpairedPrimaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      pairedFirstPrimaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      pairedSecondPrimaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      unpairedSecondaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      pairedFirstSecondaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      pairedSecondSecondaryMappedReads: Seq[ADAMRecord] = Seq.empty,
                      unmappedReads: Seq[ADAMRecord] = Seq.empty) {
  def allReads(): Seq[ADAMRecord] =
    unpairedPrimaryMappedReads ++
      pairedFirstPrimaryMappedReads ++
      pairedSecondPrimaryMappedReads ++
      unpairedSecondaryMappedReads ++
      pairedFirstSecondaryMappedReads ++
      pairedSecondSecondaryMappedReads ++
      unmappedReads
}


class ReadBucketSerializer extends Serializer[ReadBucket] {
  val recordSerializer = new AvroSerializer[ADAMRecord]()

  def writeArray(kryo: Kryo, output: Output, reads: Seq[ADAMRecord]): Unit = {
    output.writeInt(reads.size, true)
    for (read <- reads) {
      recordSerializer.write(kryo, output, read)
    }
  }

  def readArray(kryo: Kryo, input: Input): Seq[ADAMRecord] = {
    val numReads = input.readInt(true)
    (0 until numReads).foldLeft(List[ADAMRecord]()) {
      (a, b) => recordSerializer.read(kryo, input, classOf[ADAMRecord]) :: a
    }
  }

  def write(kryo: Kryo, output: Output, bucket: ReadBucket) = {
    writeArray(kryo, output, bucket.unpairedPrimaryMappedReads)
    writeArray(kryo, output, bucket.pairedFirstPrimaryMappedReads)
    writeArray(kryo, output, bucket.pairedSecondPrimaryMappedReads)
    writeArray(kryo, output, bucket.unpairedSecondaryMappedReads)
    writeArray(kryo, output, bucket.pairedFirstSecondaryMappedReads)
    writeArray(kryo, output, bucket.pairedSecondSecondaryMappedReads)
    writeArray(kryo, output, bucket.unmappedReads)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReadBucket]): ReadBucket = {
    val unpairedPrimaryReads = readArray(kryo, input)
    val pairedFirstPrimaryMappedReads = readArray(kryo, input)
    val pairedSecondPrimaryMappedReads = readArray(kryo, input)
    val unpairedSecondaryReads = readArray(kryo, input)
    val pairedFirstSecondaryMappedReads = readArray(kryo, input)
    val pairedSecondSecondaryMappedReads = readArray(kryo, input)
    val unmappedReads = readArray(kryo, input)
    new ReadBucket(
      unpairedPrimaryReads,
      pairedFirstPrimaryMappedReads,
      pairedSecondPrimaryMappedReads,
      unpairedSecondaryReads,
      pairedFirstSecondaryMappedReads,
      pairedSecondSecondaryMappedReads,
      unmappedReads)
  }
}


object ReadBucket {
  implicit def singleReadBucketToReadBucket(bucket: SingleReadBucket): ReadBucket = {
    val (pairedPrimary, unpairedPrimary) = bucket.primaryMapped.partition(_.getReadPaired)
    val (pairedFirstPrimary, pairedSecondPrimary) = pairedPrimary.partition(_.getFirstOfPair)
    val (pairedSecondary, unpairedSecondary) = bucket.secondaryMapped.partition(_.getReadPaired)
    val (pairedFirstSecondary, pairedSecondSecondary) = pairedSecondary.partition(_.getFirstOfPair)

    new ReadBucket(unpairedPrimary,
      pairedFirstPrimary,
      pairedSecondPrimary,
      unpairedSecondary,
      pairedFirstSecondary,
      pairedSecondSecondary,
      bucket.unmapped)
  }
}


trait BucketComparisons {
  /**
   * Name of the comparison. Should be identifiable, while also being able to be written on the command line.
   */
  def name: String

  /**
   * Description of the comparison, to be used by the "list comparisons" CLI option.
   */
  def description : String

  /**
   * All of the schemas which this comparison uses.
   */
  def schemas: Seq[FieldValue]

  /**
   * The records have been matched by their names, but the rest may be mismatched.
   */
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any

  /**
   * An initial value for the aggregation
   */
  def initialValue: Any

  /**
   * Aggregation function to combine the result of a computation with prior results.
   */
  def combine(first: Any, second: Any): Any

  /**
   * Write the output
   */
  def write(value: Any, stream: Writer)
}

trait IntBucketComparisons extends BucketComparisons {
  def initialValue = 0

  def combine(first: Any, second: Any): Any = first.asInstanceOf[Int] + second.asInstanceOf[Int]

  def write(value: Any, stream: Writer) {
    val int = value.asInstanceOf[Int]
    stream.append("Count\n")
    stream.append(int.toString)
    stream.append("\n")
  }
}

trait BooleanBucketComparisons extends IntBucketComparisons {
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any =
    if (boolMatchedByName(bucket1, bucket2)) 1
    else 0

  def boolMatchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Boolean
}

trait HistogramBucketComparisons[T] extends BucketComparisons {
  def combine(first: Any, second: Any): Any = first.asInstanceOf[Histogram[T]] ++ second.asInstanceOf[Histogram[T]]

  def write(value: Any, stream: Writer) {
    val histogram = value.asInstanceOf[Histogram[T]]
    stream.append("Value\tCount\n")
    histogram.valueToCount.foreach (vc => {
      stream.append("%s\t%s\n".format(vc._1, vc._2))
    })
  }
}

trait LongHistogramBucketComparisons extends HistogramBucketComparisons[Long] {
  def initialValue = Histogram[Long]()
}

trait DistanceBucketComparisons extends LongHistogramBucketComparisons {
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any =
    Histogram[Long](longMatchedByName(bucket1, bucket2))

  def longMatchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Long
}

trait PointsBucketComparisons[T] extends BucketComparisons {
  def initialValue = Points[T]()

  def combine(first: Any, second: Any): Any = first.asInstanceOf[Points[T]] ++ second.asInstanceOf[Points[T]]

  def write(value: Any, stream: Writer) {
    val points = value.asInstanceOf[Points[T]]
    stream.append("Point-x\tPoint-y\tCount\n")
    points.points.foreach (pc => {
      val point = pc._1
      val count = pc._2
      stream.append("%s\t%s\t%s\n".format(point._1, point._2, count))
    })
  }
}

trait PointBucketComparisons[T] extends PointsBucketComparisons[T] {
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Any = {
    val points = pointMatchedByName(bucket1, bucket2)
    Points[T](points)
  }

  def pointMatchedByName(bucket1: ReadBucket, bucket2: ReadBucket): (T, T)
}