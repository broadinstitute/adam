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

import edu.berkeley.cs.amplab.adam.models.SingleReadBucket
import edu.berkeley.cs.amplab.adam.util.{Histogram, Points}

abstract class MatchGenerator {
  def name: String

  /**
   * The records have been matched by their names, but the rest may be mismatched.
   */
  def matchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Any

  def initialValue: Any

  def combine(first: Any, second: Any): Any
}

abstract class IntGenerator extends MatchGenerator {
  def initialValue = 0

  def combine(first: Any, second: Any): Any = first.asInstanceOf[Int] + second.asInstanceOf[Int]
}

abstract class BooleanGenerator extends IntGenerator {
  def matchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Any =
    if (boolMatchedByName(bucket1, bucket2)) 1
    else 0

  def boolMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Boolean
}

abstract class HistogramGenerator[T] extends MatchGenerator {
  def combine(first: Any, second: Any): Any = first.asInstanceOf[Histogram[T]] ++ second.asInstanceOf[Histogram[T]]
}

abstract class LongHistogramGenerator extends HistogramGenerator[Long] {
  def initialValue = Histogram[Long]()
}

abstract class DistanceGenerator extends LongHistogramGenerator {
  def matchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Any =
    Histogram[Long](longMatchedByName(bucket1, bucket2))

  def longMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Long
}

abstract class PointsGenerator[T] extends MatchGenerator {
  def initialValue = Points[T]()

  def combine(first: Any, second: Any): Any = first.asInstanceOf[Points[T]] ++ second.asInstanceOf[Points[T]]
}

abstract class PointGenerator[T] extends PointsGenerator[T] {
  def matchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Any = {
    val points = pointMatchedByName(bucket1, bucket2)
    Points[T](points)
  }

  def pointMatchedByName(bucket1: SingleReadBucket, bucket2: SingleReadBucket): (T, T)
}