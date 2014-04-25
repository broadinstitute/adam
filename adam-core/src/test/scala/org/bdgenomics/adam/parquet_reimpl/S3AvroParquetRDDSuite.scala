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

    assert(rdd.count() === 51)
  }
}
