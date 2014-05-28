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

import org.bdgenomics.adam.models.ReferenceRegion
import java.io._
import scala.io.Source
import scala.Some
import org.bdgenomics.adam.parquet_reimpl.{ FileLocator, ByteAccess }

class RangeIndex(val entries: Array[RangeIndexEntry]) extends RowGroupIndex[RangeIndexEntry] {
  def this(itr: Iterator[RangeIndexEntry]) = this(itr.toArray)
  def this(itr: Iterable[RangeIndexEntry]) = this(itr.toArray)
  def this(is: InputStream) = this(Source.fromInputStream(is).getLines().map(RangeIndex.parseRangeIndexEntry))
  def this(file: File) = this(new FileInputStream(file))
  def this(io: ByteAccess) = this(io.readByteStream(0, io.length().toInt))
  def this(io: FileLocator) = this(io.bytes)

  override def findIndexEntries(predicate: IndexEntryPredicate[RangeIndexEntry]): Iterable[RangeIndexEntry] = {
    entries.filter(predicate.accepts)
  }
}

class RangeIndexWriter(os: OutputStream) extends RowGroupIndexWriter[RangeIndexEntry] {
  def this(f: File) = this(new FileOutputStream(f))
  private val printer: PrintWriter = new PrintWriter(os)

  override def write(entry: RangeIndexEntry) {
    printer.println(entry.line)
  }
  override def close(): Unit = {
    printer.close()
  }

  def flush() {
    printer.flush()
  }
}

object RangeIndex {
  private val referenceRegionRegex = "([^:]+):(\\d+)-(\\d+)".r

  def parseRegion(regionString: String): ReferenceRegion = {
    referenceRegionRegex.findFirstMatchIn(regionString) match {
      case Some(m) => ReferenceRegion(m.group(1), m.group(2).toLong, m.group(3).toLong)
      case None    => throw new IllegalArgumentException("\"%s\" doesn't match reference region regex".format(regionString))
    }
  }

  def parseRangeIndexEntry(line: String): RangeIndexEntry = {
    val array = line.split("\t")
    val path = array(0)
    val index = array(1).toInt
    val ranges: Seq[ReferenceRegion] = array(2).split(",").map(parseRegion).toSeq
    new RangeIndexEntry(path, index, ranges)
  }
}

/**
 * Query the entries of a range index by overlap with a query range.
 * @param queryRange
 */
case class RangeIndexPredicate(queryRange: ReferenceRegion) extends IndexEntryPredicate[RangeIndexEntry] {
  override def accepts(entry: RangeIndexEntry): Boolean =
    entry.ranges.exists(_.overlaps(queryRange))
}

case class RangeIndexEntry(path: String, rowGroupIndex: Int, ranges: Seq[ReferenceRegion])
    extends RowGroupIndexEntry(path, rowGroupIndex) {

  def stringifyRange(range: ReferenceRegion): String = "%s:%d-%d".format(range.referenceName, range.start, range.end)

  def line: String = {
    "%s\t%d\t%s".format(path, rowGroupIndex, ranges.map(stringifyRange).mkString(","))
  }
}
