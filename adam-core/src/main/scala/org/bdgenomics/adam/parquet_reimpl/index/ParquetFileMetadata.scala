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

import org.bdgenomics.adam.parquet_reimpl.{ ParquetPartition, ParquetSchemaType, FileLocator }
import org.bdgenomics.adam.rdd.{ ParquetCommon, Footer }
import parquet.format.FileMetaData
import org.apache.avro.Schema
import parquet.schema.MessageType
import parquet.avro.AvroSchemaConverter

case class ParquetFileMetadata(locator: FileLocator,
                               footer: Footer,
                               metadata: FileMetaData,
                               requested: ParquetSchemaType,
                               actualSchema: ParquetSchemaType) {

  def partition(index: Int): ParquetPartition = {
    val rowGroup = footer.rowGroups(index)
    new ParquetPartition(locator, index, rowGroup, requested, actualSchema)
  }
}

object AvroParquetFileMetadata {

  def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
    schema match {
      case None    => fileMessageType
      case Some(s) => new AvroSchemaConverter().convert(s)
    }

  def apply(parquetLocator: FileLocator, requestedSchema: Option[Schema]): ParquetFileMetadata = {

    val fileMetadata = ParquetCommon.readFileMetadata(parquetLocator.bytes)
    val footer = new Footer(fileMetadata)
    val fileMessageType = ParquetCommon.parseMessageType(fileMetadata)

    // TODO: is this actually necessary?  do we actually just want to re-use the fileMessageType here?
    val requestedMessage = convertAvroSchema(requestedSchema, fileMessageType)
    val requested = new ParquetSchemaType(requestedMessage)
    val actual = new ParquetSchemaType(fileMessageType)

    ParquetFileMetadata(parquetLocator, footer, fileMetadata, requested, actual)
  }
}

