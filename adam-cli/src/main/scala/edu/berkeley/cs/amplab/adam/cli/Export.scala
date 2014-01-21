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
package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.kohsuke.args4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.{SequenceDictionary, ReferencePosition, SequenceRecord}
import org.apache.spark.rdd.RDD
import net.sf.samtools.{SAMFileReader, SAMFileHeader, SAMFileWriter, BAMFileWriter}
import java.io.File
import edu.berkeley.cs.amplab.adam.rdd.GenomicRegionPartitioner
import edu.berkeley.cs.amplab.adam.converters.SAMRecordConverter
import org.apache.hadoop.fs.Path

object Export extends AdamCommandCompanion {
  val commandName: String = "export"
  val commandDescription: String = "Exports the contents of an ADAM file in the specified format"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new Export(Args4j[ExportArgs](cmdLine))
  }
}

/**
 * TODO add an option here to choose different output formats.
 */
class ExportArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to export", index = 0)
  val inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The file to output", index = 1)
  val outputPath: String = null

  @Option(required = false, name = "-parts", usage = "Number of ways to parallelize the write (default=20)")
  var parts = 20
}

/**
 * Reads in the BAMs (*in sorted order*, by filename) and concatenates them.
 *
 * How many other kinds of export are going to need this step?  I can think of at least two:
 * FAST{A,Q}, and VCF. Others?
 */
object BAMCombiner {

  def combineBAMs(header : SAMFileHeader, outputPath : String, filenames : Iterable[String]) {

    // TODO again, probably need to create a Path and an outputStream here...
    val writer = new BAMFileWriter(new File("%s.bam".format(outputPath)))
    writer.setSortOrder(SAMFileHeader.SortOrder.coordinate, true)
    writer.setHeader(header)

    for( partFilename <- filenames ) {
      val reader = new SAMFileReader(new File(partFilename))

      val itr = reader.iterator()
      for( samRecord <- itr ) {
        writer.addAlignment(samRecord)
      }

      reader.close()
    }

    writer.close()
  }
}

/**
 * TODO need to combine this with ExportFastq command, into a single "uber" export.
 *
 * @param args
 */
class Export(protected val args: ExportArgs) extends AdamSparkCommand[ExportArgs] {
  val companion: AdamCommandCompanion = Export

  def run(sc: SparkContext, job: Job): Unit = {
    // Quiet parquet logging...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)


    /**
     * BAM Export
     */

    // first, we need the dictionary to both build the SAMFileHeader as well
    // as create the genomic region partitioner
    val dict : SequenceDictionary = sc.adamDictionaryLoad(args.inputPath)
    val partitioner = new GenomicRegionPartitioner(args.parts, dict)

    // Next, we create a serializable version of the header that can be passed
    // to each node as it processes each partition.

    // TODO we're going to run into problems, because I don't think that SAMFileHeader
    // TODO is serializable (*sigh*).  So, we're going to need to wrap this data into an
    // TODO equivalent, serializable piece -- and pass *that* into each partition writer, and
    // TODO reconstruct the header inside each writer.  BLARGH!!
    val header = sc.prepareSAMFileHeader(args.inputPath)
    // TODO fill in the header here

    val reads : RDD[ADAMRecord] = sc.adamLoad(args.inputPath)

    // Sort the reads and key them by position -- this is critical, because we're going
    // to generate a separate BAM for each partition.  Open question: is it fast to
    // key-then-sort-within-each-partition, or sort-then-key?  Does it matter?
    // Follow-up question: If I sort then partition, does it maintain the sort order within
    // each partition?  Hmmmm...
    // TODO this won't handle unmapped reads.
    // TODO check that GenomicRegionPartitioner is doing the right thing relative to reads which span partition blocks
    val sorted = reads.adamSortReadsByReferencePosition().keyBy {
      rec =>
        ReferencePosition(rec.getReferenceId, rec.getStart)
    }.partitionBy(partitioner)

    // Because we can't guarantee total ordering on calls to (e.g.) map,
    // and because we are unable to ensure that a call to collect() will be
    // able to hold all the reads in memory (and, honestly, it most likely
    // won't), we have to do the next best thing: partition, and output a separate
    // BAM for each partition -- we can use the filename to define a global sort
    // order, so that we don't have to write a generalized merge sort to combine
    // the BAMs.
    val filenames = sorted.mapPartitions {
      itr : Iterator[(ReferencePosition,ADAMRecord)] => {

        var filename : String = null
        var writer : BAMFileWriter = null

        // TODO Kinda wish SAMRecordConverter was an object instead of a class.
        val sam = new SAMRecordConverter()

        for( (pos, rec) <- itr ) {
          // TODO see note above -- probably want to check that we're still in coordinate-sorted order here.

          if(writer == null) {

            // we don't know "where" each partition *is* in the genome, until we
            // see the first read, and we need that partition location to generate
            // the filename and (ultimately) the BAMFileWriter for that filename.
            // Therefore, we can't initialize the writer until we see the first
            // read...
            filename = "%s_%d_%d.bam".format(args.outputPath, rec.getReferenceId.toInt, rec.getStart.toInt)
            val path = new Path(filename)

            // TODO create this out of a file outputstream, rather than directly to local disk.
            writer = new BAMFileWriter(new File(filename))
            writer.setSortOrder(SAMFileHeader.SortOrder.coordinate, true)
            writer.setHeader(header)
          }

          writer.addAlignment(sam.unconvert(rec, header))
        }

        if(writer != null) {
          writer.close()
          Seq(filename).iterator

        } else {
          Seq().iterator
        }
      }
    }

    BAMCombiner.combineBAMs(header, args.outputPath, filenames.collect().sorted)
  }

}
