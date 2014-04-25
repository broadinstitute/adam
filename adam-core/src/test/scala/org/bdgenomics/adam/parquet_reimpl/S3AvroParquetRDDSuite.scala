/*
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
package org.bdgenomics.adam.parquet_reimpl

import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.util.SparkFunSuite
import com.amazonaws.services.s3.AmazonS3Client
import org.bdgenomics.adam.parquet_reimpl.S3AvroParquetRDD
import org.bdgenomics.adam.projections.Projection
import parquet.filter.{ RecordFilter, UnboundRecordFilter }
import java.lang.Iterable
import parquet.column.ColumnReader

import scala.collection.JavaConversions._
import parquet.io.api.Binary
import parquet.filter.ColumnRecordFilter._
import scala.Some
import parquet.filter.ColumnPredicates._
import scala.Some

class S3AvroParquetRDDSuite extends SparkFunSuite {
  sparkTest("Try pulling out a coupla records from a parquet file") {
    val rdd = new S3AvroParquetRDD[ADAMRecord](
      sc,
      null,
      "genomebridge-variantstore-ci",
      "demo/reads12.adam/part1",
      None)

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 51)
  }

  sparkTest("Using a projection works") {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, sequence)

    val rdd = new S3AvroParquetRDD[ADAMRecord](
      sc,
      null,
      "genomebridge-variantstore-ci",
      "demo/reads12.adam/part1",
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 51)
  }

  sparkTest("Using a filter also works") {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, sequence)
    val filter = new ReadNameFilter("simread:1:189606653:true")

    val rdd = new S3AvroParquetRDD[ADAMRecord](
      sc,
      filter,
      "genomebridge-variantstore-ci",
      "demo/reads12.adam/part1",
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.collect().length === 1)
    assert(rdd.count() === 1)
  }

}

class ReadNameFilter(value: String) extends UnboundRecordFilter with Serializable {

  def bind(readers: Iterable[ColumnReader]): RecordFilter = {
    //println("Bind: " + readers.map(cr => cr.getDescriptor.getPath.mkString(".")).mkString(","))
    //Thread.dumpStack()
    val reader = readers.find(_.getDescriptor.getPath.last == "readName").get
    new RecordFilter() {
      def isMatch: Boolean = {
        val mtch = reader.getBinary.toStringUsingUTF8 == value
        //println("%s -> %s".format(reader.getBinary.toStringUsingUTF8, mtch))
        mtch
      }
    }
  }
}

