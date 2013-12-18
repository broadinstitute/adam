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

case class Points[T](points: Map[(T, T), Int]) {
  def ++(other: Points[T]): Points[T] = {
    val map = collection.mutable.HashMap[(T, T), Int]()
    points foreach map.+=
    other.points foreach(kv => {
      val newValue = map.getOrElse(kv._1, 0) + kv._2
      map(kv._1) = newValue
    })
    new Points[T](map.toMap)
  }
}

object Points {
  def apply[T](): Points[T] = new Points[T](Map())
  def apply[T](points: (T, T)*): Points[T] = new Points[T](Map(points.map((_, 1)): _*))
}