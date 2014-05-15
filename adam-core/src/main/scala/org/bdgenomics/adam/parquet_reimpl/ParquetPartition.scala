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

import parquet.filter.UnboundRecordFilter
import parquet.io.api.RecordMaterializer
import parquet.io.ColumnIOFactory
import parquet.column.page.{ PageReadStore, PageReader }
import parquet.column.ColumnDescriptor
import org.apache.hadoop.io.compress.{ CompressionCodec => HadoopCompressionCodec }
import org.apache.spark.Partition
import org.bdgenomics.adam.rdd._
import org.bdgenomics.adam.rdd.ParquetColumnDescriptor
import org.bdgenomics.adam.rdd.ParquetRowGroup
import org.apache.hadoop.conf.Configuration

class ParquetPartition(val index: Int,
                       val rowGroup: ParquetRowGroup,
                       val requestedSchema: ParquetSchemaType,
                       val actualSchema: ParquetSchemaType)
    extends Partition {

  def materializeRecords[T](io: ByteAccess,
                            recordMaterializer: RecordMaterializer[T],
                            filter: UnboundRecordFilter): Iterator[T] =
    ParquetPartition.materializeRecords(io, recordMaterializer, filter, rowGroup, requestedSchema, actualSchema)

}

class PartitionPageReadStore(chunkMap: Map[ParquetColumnDescriptor, PageReader], rowGroup: ParquetRowGroup)
    extends PageReadStore {

  override def getPageReader(cd: ColumnDescriptor): PageReader =
    chunkMap
      .get(new ParquetColumnDescriptor(cd))
      .getOrElse(
        throw new NoSuchElementException("Could not find %s in the map %s".format(cd.getPath.mkString("."), chunkMap.keys.map(_.path.mkString(".")).mkString(","))))
  override def getRowCount: Long = rowGroup.rowCount
}

object ParquetPartition {

  def materializeRecords[T](io: ByteAccess,
                            recordMaterializer: RecordMaterializer[T],
                            filter: UnboundRecordFilter,
                            rowGroup: ParquetRowGroup,
                            requestedSchema: ParquetSchemaType,
                            actualSchema: ParquetSchemaType): Iterator[T] = {

    val requestedPaths = requestedSchema.paths()

    val requestedColumnChunks: Seq[ParquetColumnChunk] = rowGroup.columnChunks.filter {
      cc => requestedPaths.contains(TypePath(cc.columnDescriptor.path))
    }

    val config: Configuration = new Configuration()

    val decompressor: Option[HadoopCompressionCodec] =
      CompressionCodecEnum.getHadoopCodec(rowGroup.columnChunks.head.compressionCodec, config)

    val chunkMap = requestedColumnChunks
      .map(cc => (cc.columnDescriptor, cc.readAllPages(decompressor, io)))
      .toMap
    val pageReadStore = new PartitionPageReadStore(chunkMap, rowGroup)

    val columnIOFactory: ColumnIOFactory = new ColumnIOFactory
    val columnIO = columnIOFactory.getColumnIO(requestedSchema.convertToParquet(), actualSchema.convertToParquet())
    val reader = columnIO.getRecordReader[T](pageReadStore, recordMaterializer, filter)

    new Iterator[T] {
      var recordsRead = 0
      val totalRecords = rowGroup.rowCount
      var nextT: Option[T] = Option(reader.read())

      override def next(): T = {
        val ret = nextT
        recordsRead += 1
        if (recordsRead >= totalRecords) {
          nextT = None
        } else {
          nextT = Option(reader.read())
        }
        ret.getOrElse(null.asInstanceOf[T])
      }

      override def hasNext: Boolean = nextT.isDefined
    }

  }
}
