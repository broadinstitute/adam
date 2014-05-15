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

import org.scalatest.FunSuite
import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.bdgenomics.adam.avro.ADAMFlatGenotype
import org.bdgenomics.adam.models.ReferenceRegion

class RangeIndexGeneratorSuite extends FunSuite {

  test("small_adam.fgenotype is indexed into one RangeIndexEntry") {

    val generator = new RangeIndexGenerator[ADAMFlatGenotype]

    val parquetPath = Thread.currentThread().getContextClassLoader.getResource("small_adam.fgenotype").getFile
    val entrySeq = generator.addParquetFile(parquetPath).toSeq

    assert(entrySeq.size === 1)

    entrySeq.head match {
      case RangeIndexEntry(path, index, ranges) => {
        assert(path === parquetPath)
        assert(index === 0)
        assert(ranges.size === 3)
        ranges.head match {
          case ReferenceRegion(refName, start, end) => {
            assert(refName === "chr1")
            assert(start === 14397)
            assert(end === 19190)
          }
        }
      }
    }

  }
}