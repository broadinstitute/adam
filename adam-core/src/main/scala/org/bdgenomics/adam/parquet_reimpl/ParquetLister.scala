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

import java.io._
import org.bdgenomics.adam.rdd._
import parquet.avro.{ AvroSchemaConverter, UsableAvroRecordMaterializer }
import parquet.schema.MessageType
import scala.reflect._
import org.apache.avro.Schema
import parquet.filter.UnboundRecordFilter
import org.bdgenomics.adam.rdd.ParquetRowGroup
import parquet.io.api.RecordMaterializer
import org.apache.avro.generic.IndexedRecord
import org.apache.spark.Logging

class ParquetLister[T <: IndexedRecord](indexableSchema: Option[Schema] = None)(implicit classTag: ClassTag[T])
    extends Logging {

  val avroSchema: Schema = classTag.runtimeClass.newInstance().asInstanceOf[T].getSchema
  val filter: UnboundRecordFilter = null

  def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
    schema match {
      case None    => fileMessageType
      case Some(s) => new AvroSchemaConverter().convert(s)
    }

  def materialize(rowGroup: ParquetRowGroup,
                  io: ByteAccess,
                  materializer: RecordMaterializer[T],
                  reqSchema: ParquetSchemaType,
                  actualSchema: ParquetSchemaType): Iterator[T] =
    ParquetPartition.materializeRecords(io, materializer, filter, rowGroup, reqSchema, actualSchema)

  def materialize(fullPath: String): Iterator[T] = {
    val file = new File(fullPath)
    val rootLocator = new LocalFileLocator(file.getParentFile)
    val relativePath = file.getName
    if (file.isFile) {
      logInfo("Indexing file %s, relative path %s".format(fullPath, relativePath))
      materialize(rootLocator, relativePath)
    } else {
      val childFiles = file.listFiles().filter(f => f.isFile && !f.getName.startsWith("."))
      childFiles.flatMap {
        case f => {
          val childRelativePath = "%s/%s".format(relativePath, f.getName)
          logInfo("Indexing child file %s, relative path %s".format(f.getName, childRelativePath))
          materialize(rootLocator, childRelativePath)
        }
      }.iterator
    }
  }

  def materialize(rootLocator: FileLocator, relativePath: String): Iterator[T] = {
    val locator = rootLocator.relativeLocator(relativePath)
    val io = locator.bytes
    val footer: Footer = ParquetCommon.readFooter(io)
    val fileMessageType: MessageType = ParquetCommon.parseMessageType(ParquetCommon.readFileMetadata(io))
    val actualSchema: ParquetSchemaType = new ParquetSchemaType(fileMessageType)
    val requestedMessageType: MessageType = convertAvroSchema(indexableSchema, fileMessageType)
    val reqSchema: ParquetSchemaType = new ParquetSchemaType(requestedMessageType)
    val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, avroSchema)

    logInfo("# row groups: %d".format(footer.rowGroups.length))
    logInfo("# total records: %d".format(footer.rowGroups.map(_.rowCount).sum))

    footer.rowGroups.zipWithIndex.flatMap {
      case (rowGroup: ParquetRowGroup, i: Int) => {
        logInfo("row group %d, # records %d".format(i, rowGroup.rowCount))
        materialize(rowGroup, io, avroRecordMaterializer, reqSchema, actualSchema)
      }
    }.iterator
  }
}

