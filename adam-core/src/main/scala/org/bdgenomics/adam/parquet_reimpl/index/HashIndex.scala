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
package org.bdgenomics.adam.parquet_reimpl.index

import java.io.File
import scala.io.Source

class HashIndex[T](file: File)(implicit converter: ValueConverter[T]) extends RowGroupIndex[HashIndexEntry[T]] {

  assert(file.exists && file.canRead, "Either file %s doesn't exist or is not readable".format(file.getAbsolutePath))

  val entries: Set[HashIndexEntry[T]] =
    Source.fromFile(file).getLines().toSet[String].map {
      case line: String => line.split("\t")
    }.map {
      case array: Array[String] => HashIndexEntry(array(0), array(1).toInt, converter.deserialize(array(2)))
    }

  override def findIndexEntries(predicate: IndexEntryPredicate[HashIndexEntry[T]]): Iterable[HashIndexEntry[T]] =
    entries.filter(predicate.accepts)
}

case class HashIndexEntry[T](path: String, index: Int, value: T) extends RowGroupIndexEntry(path, index) {
  def line: String = "%s\t%d\t%s".format(path, index, value)
}

trait ValueConverter[T] {
  def deserialize(str: String): T
  def serialize(value: T): String
}

object HashIndexConverters {

  implicit val stringConverter: ValueConverter[String] = new ValueConverter[String] {
    override def deserialize(str: String): String = str
    override def serialize(value: String): String = value
  }

  implicit val longConverter: ValueConverter[Long] = new ValueConverter[Long] {
    override def deserialize(str: String): Long = str.toLong
    override def serialize(value: Long): String = value.toString
  }
}

