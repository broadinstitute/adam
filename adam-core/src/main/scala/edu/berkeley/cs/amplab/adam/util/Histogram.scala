/**
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

package edu.berkeley.cs.amplab.adam.util

case class Histogram[T](valueToCount: Map[T, Int]) {
  def ++(other: Histogram[T]): Histogram[T] = {
    val map = collection.mutable.HashMap[T, Int]()
    valueToCount foreach map.+=
    other.valueToCount foreach(kv => {
      val newValue = map.getOrElse(kv._1, 0) + kv._2
      map(kv._1) = newValue
    })
    new Histogram[T](map.toMap)
  }
}

object Histogram {
  def apply[T]() : Histogram[T] = {
    new Histogram[T](Map())
  }

  def apply[T](value: T): Histogram[T] = {
    new Histogram[T](Map((value, 1)))
  }
}