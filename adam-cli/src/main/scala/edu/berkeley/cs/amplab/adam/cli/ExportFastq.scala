/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.cli

import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level

import edu.berkeley.cs.amplab.adam.projections.{Projection,ADAMRecordField}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.rdd.RDD
import java.io._
import net.sf.picard.fastq.{BasicFastqWriter, FastqWriter, FastqRecord}
import java.util.zip.GZIPOutputStream

// TODO: should permute the ordering of the reads.
// TODO: split the reads by sample / library / read group
// TODO: add tests.

object ExportFastq extends AdamCommandCompanion with Serializable {
  val commandName: String = "export_fastq"
  val commandDescription: String = "Exports a gzipped FASTQ file from an ADAM file"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new ExportFastq(Args4j[ExportFastqArgs](cmdLine))
  }

  def createGzipFastqWriter(fileFormat : String, partitionIdx : Int) : FastqWriter = {
    val file = new File(fileFormat.format(partitionIdx))
    val ps : PrintStream = new PrintStream(new GZIPOutputStream(new FileOutputStream(file)))
    new SerializableFastqWriter(ps)
  }

  def fastqRecord(rec : ADAMRecord) : FastqRecord = {
    new FastqRecord(rec.getReadName, rec.getSequence, "", rec.getQual)
  }
}

class ExportFastqArgs extends Args4jBase with SparkArgs with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to export", index = 0)
  val inputPath: String = null
}

class ExportFastq(protected val args: ExportFastqArgs) extends AdamSparkCommand[ExportFastqArgs] with Serializable {
  val companion: AdamCommandCompanion = ExportFastq

  def run(sc: SparkContext, job: Job): Unit = {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val singlesFile = "singles.%d.fasta.gz"
    val pairs1File = "pairs_1.%d.fasta.gz"
    val pairs2File = "pairs_2.%d.fasta.gz"

    val proj = Projection(
      ADAMRecordField.readName,
      ADAMRecordField.sequence,
      ADAMRecordField.qual,
      ADAMRecordField.readPaired,
      ADAMRecordField.firstOfPair,
      ADAMRecordField.primaryAlignment,
      ADAMRecordField.recordGroupSample,
      ADAMRecordField.recordGroupLibrary,
      ADAMRecordField.recordGroupId
    )

    def printSingleRecords(fileFormat : String)(idx : Int, itr : Iterator[FastqRecord]) : Iterator[Int] = {
      var written = 0
      val writer = ExportFastq.createGzipFastqWriter(fileFormat, idx)
      itr.foreach {
        rec =>
          writer.write(rec)
          written += 1
      }
      writer.close()
      List(written).iterator
    }

    def printPairedRecords(file1Format : String, file2Format : String)(idx : Int, itr : Iterator[(String,(ADAMRecord,ADAMRecord))]) : Iterator[Int] = {
      var written = 0

      val w1 = ExportFastq.createGzipFastqWriter(file1Format, idx)
      val w2 = ExportFastq.createGzipFastqWriter(file2Format, idx)

      itr.foreach {
        case (name, (read1, read2)) =>
          val f1 = ExportFastq.fastqRecord(read1)
          val f2 = ExportFastq.fastqRecord(read2)
          w1.write(f1)
          w2.write(f2)
          written += 1
      }

      w1.close()
      w2.close()

      List(written).iterator
    }

    val reads : RDD[ADAMRecord] = sc.adamLoad(args.inputPath, projection=Some(proj))

    /*
     * First, output all the non-paired reads.
     */
    val singles = reads.filter(rec => !rec.getReadPaired && rec.getPrimaryAlignment)

    if(singles.count() > 0) {
      val written = singles.map(ExportFastq.fastqRecord).mapPartitionsWithIndex(printSingleRecords(singlesFile))
        .reduce(_+_)
      println("%d reads written".format(written))
    }

    /*
     * Next, output the paired reads.
     */
    val paired = reads.filter(rec => rec.getReadPaired && rec.getPrimaryAlignment)
    val pairedFirst : RDD[(String,ADAMRecord)] = paired.filter(_.getFirstOfPair).keyBy(_.getReadName)
    val pairedSecond : RDD[(String,ADAMRecord)] = paired.filter(!_.getFirstOfPair).keyBy(_.getReadName)

    if(pairedFirst.count() > 0) {
      val joined : RDD[(String,(ADAMRecord,ADAMRecord))] = pairedFirst.join(pairedSecond).sortByKey()
      val written = joined.mapPartitionsWithIndex(printPairedRecords(pairs1File, pairs2File)).reduce(_ + _)
      println("%d read-pairs written".format(written))
    }
  }

}

class SerializableFastqWriter(ps : PrintStream) extends BasicFastqWriter(ps) with Serializable {
}
