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
import java.io.File
import scala.io.Source


class RowGroupRangeIndex(val indexFile: File) extends RowGroupIndex[RowGroupRangeIndexEntry] {

  val entries: Seq[RowGroupRangeIndexEntry] =
    Source.fromFile(indexFile).getLines().map {
      case line: String => {
        val array = line.split("\t")
        val path = array(0)
        val index = array(1).toInt
        val ranges: Seq[ReferenceRegion] = array(2).split(",").map(RowGroupRangeIndex.parseRegion).toSeq
        new RowGroupRangeIndexEntry(path, index, ranges)
      }
    }.toSeq

  override def findIndexEntries(predicate: IndexEntryPredicate[RowGroupRangeIndexEntry]): Iterable[RowGroupRangeIndexEntry] = {
    entries.filter(predicate.accepts)
  }
}

object RowGroupRangeIndex {
  private val referenceRegionRegex = "([^:]+):(\\d+)-(\\d+)".r
  private def parseRegion(regionString: String): ReferenceRegion = {
    referenceRegionRegex.findFirstMatchIn(regionString) match {
      case Some(m) => ReferenceRegion(m.group(1), m.group(2).toLong, m.group(3).toLong)
      case None    => throw new IllegalArgumentException("\"%s\" doesn't match reference region regex".format(regionString))
    }
  }
}

/**
 * Query the entries of a range index by overlap with a query range.
 * @param queryRange
 */
case class RangeIndexPredicate(queryRange : ReferenceRegion) extends IndexEntryPredicate[RowGroupRangeIndexEntry] {
  override def accepts(entry: RowGroupRangeIndexEntry): Boolean =
    entry.ranges.exists( _.overlaps(queryRange) )
}

class RowGroupRangeIndexEntry(path: String, rowGroupIndex: Int, val ranges: Seq[ReferenceRegion])
    extends RowGroupIndexEntry(path, rowGroupIndex) {
}
