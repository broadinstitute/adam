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
package org.bdgenomics.adam.parquet_reimpl {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{ TaskContext, Partition, SparkContext }
  import collection.JavaConversions._
  import parquet.filter.UnboundRecordFilter
  import org.apache.avro.Schema
  import org.bdgenomics.adam.rdd._
  import org.apache.avro.generic.IndexedRecord
  import scala.reflect._
  import parquet.avro.{ UsableAvroRecordMaterializer, AvroSchemaConverter }
  import parquet.schema.MessageType
  import org.bdgenomics.adam.parquet_reimpl.index._
  import org.bdgenomics.adam.parquet_reimpl.filters.CombinedFilter
  import parquet.format.FileMetaData
import org.bdgenomics.adam.parquet_reimpl.index.ParquetFileMetadata
import scala.Some
import org.bdgenomics.adam.parquet_reimpl.index.RangeIndexEntry

class S3AvroIndexedParquetRDD[T <: IndexedRecord: ClassTag](@transient sc: SparkContext,
                                                              private val filter: CombinedFilter[T, IDRangeIndexEntry],
                                                              private val indexLocator: FileLocator,
                                                              private val dataRootLocator: FileLocator,
                                                              @transient private val requestedSchema: Option[Schema])

      extends RDD[T](sc, Nil) {

    override protected def getPartitions: Array[Partition] = {
      val index: IDRangeIndex = new IDRangeIndex(indexLocator.bytes)

      /**
       * There are three steps here:
       *
       * 1. find the relevant index entries from the index
       * 2. find the distinct parquet files from the corresponding entries, and load their
       *    metadata.
       * 3. use the metadata of the parquet files to construct the corresponding ParquetPartition
       *    objects.
       */

      /*
    Step 1: find the relevant index entries
     */
      val entries: Iterable[IDRangeIndexEntry] = index.findIndexEntries(filter.indexPredicate)

      /*
    Step 2: get the parquet file metadata.
     */
      val parquetFiles: Map[String, ParquetFileMetadata] = entries.map(_.path).toSeq.distinct.map {
        case parquetFilePath: String => {
          val parquetLocator = dataRootLocator.relativeLocator(parquetFilePath)
          parquetFilePath -> AvroParquetFileMetadata(parquetLocator, requestedSchema)
        }
      }.toMap

      /*
    Step 3: build the ParquetPartition values.
     */
      entries.toArray.map {
        case IDRangeIndexEntry(path, i, sample, range) => parquetFiles(path).partition(i)
      }
    }

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      val reqSchema = classTag[T].runtimeClass.newInstance().asInstanceOf[T].getSchema
      val parquetPartition = split.asInstanceOf[ParquetPartition]
      val byteAccess = parquetPartition.locator.bytes
      def requestedMessageType = parquetPartition.requestedSchema.convertToParquet()

      val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, reqSchema)

      parquetPartition.materializeRecords(byteAccess, avroRecordMaterializer, filter.recordFilter)
    }
  }

  /**
   * This class is very specific for an S3 Avro Parquet RDD.
   */
  class S3AvroParquetRDD[T <: IndexedRecord: ClassTag](@transient sc: SparkContext,
                                                       private val filter: UnboundRecordFilter,
                                                       private val parquetFile: FileLocator,
                                                       @transient private val requestedSchema: Option[Schema])
      extends RDD[T](sc, Nil) {

    assert(requestedSchema != null, "Use \"None\" instead of null for no schema.")

    def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
      schema match {
        case None    => fileMessageType
        case Some(s) => new AvroSchemaConverter().convert(s)
      }

    def io(): ByteAccess = parquetFile.bytes

    override protected def getPartitions: Array[Partition] = {

      val fileMetadata = ParquetCommon.readFileMetadata(io())
      val footer = new Footer(fileMetadata)
      val fileMessageType = ParquetCommon.parseMessageType(fileMetadata)
      val fileSchema = new ParquetSchemaType(fileMessageType)
      val requestedMessage = convertAvroSchema(requestedSchema, fileMessageType)
      val requested = new ParquetSchemaType(requestedMessage)

      /*
    println("Requested Schema: \n" + requestedSchema)
    println("Requested MessageType: \n" + requestedMessage.toString)
    println("Requested: \n" + requested.toString)
    println("Actual: \n" + fileSchema.toString)
    println("Original Avro Schema:\n" + new AvroSchemaConverter().convert(fileMessageType))
    */

      footer.rowGroups.zipWithIndex.map {
        case (rg, i) => new ParquetPartition(parquetFile, i, rg, requested, fileSchema)
      }.toArray
    }

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      val reqSchema = classTag[T].runtimeClass.newInstance().asInstanceOf[T].getSchema
      val byteAccess = io()
      val parquetPartition = split.asInstanceOf[ParquetPartition]
      def requestedMessageType = parquetPartition.requestedSchema.convertToParquet()

      //val reqSchema = new AvroSchemaConverter().convert(requestedMessageType)
      //val reqSchema = new AvroSchemaConverter().convert(parquetPartition.actualSchema.convertToParquet())
      //println("Base Avro Schema:\n" + reqSchema)

      val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, reqSchema)

      parquetPartition.materializeRecords(byteAccess, avroRecordMaterializer, filter)
    }
  }
}

package parquet.avro {

  import parquet.io.api.{ GroupConverter, RecordMaterializer }
  import org.apache.avro.Schema
  import parquet.schema.MessageType

  /**
   * Once again, OMGWTF, Parquet has put AvroRecordMaterializer and (even worse) AvroIndexedRecordConverter
   * as package-private classes in parquet.avro.  AWER@#$GF@#$!~ASAS!!!1!
   *
   * @tparam T
   */
  class UsableAvroRecordMaterializer[T <: org.apache.avro.generic.IndexedRecord](root: AvroIndexedRecordConverter[T]) extends RecordMaterializer[T] {
    def this(requestedSchema: MessageType, avroSchema: Schema) =
      this(new AvroIndexedRecordConverter[T](requestedSchema, avroSchema))

    def getCurrentRecord: T = root.getCurrentRecord
    def getRootConverter: GroupConverter = root
  }
}
