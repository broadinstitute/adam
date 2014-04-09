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
package org.bdgenomics.adam.parquet_reimpl

import com.amazonaws.services.s3.AmazonS3Client
import org.scalatest.FunSuite
import java.io.File
import org.scalatest.FunSuite
import org.bdgenomics.adam.rdd.{ ParquetCommon }

class ParquetCommonSuite extends FunSuite {

  val filename = Thread.currentThread().getContextClassLoader.getResource("small_adam.fgenotype").getFile
  val s3Filename = ""

  test("Can load footer from small vcf") {
    val access = new InputStreamByteAccess(new File(filename))
    val footer = ParquetCommon.readFooter(access)

    assert(footer.rowGroups.length === 1)
  }

  test("Testing S3 byte access") {
    val byteAccess = new S3ByteAccess(new AmazonS3Client(), "genomebridge-variantstore-ci", "data/flannick/chr10.adam")
    assert(byteAccess.readFully(0, 1)(0) === 80)
  }

  test("Reading a footer from S3") {
    val byteAccess = new S3ByteAccess(new AmazonS3Client(), "genomebridge-variantstore-ci", "data/flannick/chr10.adam")
    val footer = ParquetCommon.readFooter(byteAccess)
    assert(footer.rowGroups.length === 11)
  }
}
