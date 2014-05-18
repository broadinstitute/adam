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

import java.io._
import org.bdgenomics.adam.rdd._
import org.bdgenomics.adam.parquet_reimpl._
import org.bdgenomics.adam.models.{ ReferenceMapping, ReferenceRegion }
import parquet.avro.UsableAvroRecordMaterializer
import parquet.schema.MessageType
import scala.reflect._
import org.apache.avro.Schema
import org.bdgenomics.adam.parquet_reimpl.ParquetSchemaType
import parquet.filter.UnboundRecordFilter
import org.bdgenomics.adam.parquet_reimpl.ParquetSchemaType
import org.bdgenomics.adam.rdd.ParquetRowGroup
import parquet.io.api.RecordMaterializer
import org.apache.avro.generic.IndexedRecord

class RangeIndexGenerator[T <: IndexedRecord](rangeGapSize: Long = 10000L)(implicit referenceMapping: ReferenceMapping[T], classTag: ClassTag[T]) {

  val avroSchema: Schema = classTag.runtimeClass.newInstance().asInstanceOf[T].getSchema
  val filter: UnboundRecordFilter = null

  def canCombine(r1: ReferenceRegion, r2: ReferenceRegion): Boolean = {
    r1.referenceName == r2.referenceName && r1.distance(r2).get <= rangeGapSize
  }

  def folder(next: ReferenceRegion, acc: Seq[ReferenceRegion]): Seq[ReferenceRegion] = {
    acc match {
      case Seq() => Seq(next)
      case head :: tail =>
        if (canCombine(head, next)) head.hull(next) :: tail else next :: head :: tail
    }
  }

  def ranges(rowGroup: ParquetRowGroup,
             io: ByteAccess,
             materializer: RecordMaterializer[T],
             reqSchema: ParquetSchemaType,
             actualSchema: ParquetSchemaType): Seq[ReferenceRegion] =
    ParquetPartition.materializeRecords(io, materializer, filter, rowGroup, reqSchema, actualSchema).map(
      referenceMapping.getReferenceRegion).filter(_ != null).foldRight(Seq[ReferenceRegion]())(folder)

  def addParquetFile(fullPath: String): Iterator[RangeIndexEntry] = {
    val file = new File(fullPath)
    val rootLocator = new LocalFileLocator(file.getParentFile)
    addParquetFile(rootLocator, file.getName)
  }

  def addParquetFile(rootLocator: FileLocator, relativePath: String): Iterator[RangeIndexEntry] = {
    val locator = rootLocator.relativeLocator(relativePath)
    val io = locator.bytes
    val footer: Footer = ParquetCommon.readFooter(io)
    val actualSchema: ParquetSchemaType = new ParquetSchemaType(ParquetCommon.parseMessageType(ParquetCommon.readFileMetadata(io)))
    val reqSchema: ParquetSchemaType = actualSchema
    val requestedMessageType: MessageType = reqSchema.convertToParquet()
    val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, avroSchema)

    footer.rowGroups.zipWithIndex.map {
      case (rowGroup: ParquetRowGroup, i: Int) =>
        new RangeIndexEntry(relativePath, i, ranges(rowGroup, io, avroRecordMaterializer, reqSchema, actualSchema))
    }.iterator
  }
}

