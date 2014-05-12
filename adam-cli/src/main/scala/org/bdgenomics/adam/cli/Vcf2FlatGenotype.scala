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
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.util.ParquetLogger
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import net.sf.samtools._
import scala.collection.JavaConversions._
import java.io.{ FileInputStream, File }
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import java.util.concurrent._
import scala.Some
import java.util.logging.Level
import org.bdgenomics.adam.cli.{ Args4jBase, Args4j }
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.adam.avro.ADAMFlatGenotype
import org.bdgenomics.adam.converters.{ VCFLineConverter, VCFLineParser }

object Vcf2FlatGenotype extends ADAMCommandCompanion {
  val commandName: String = "vcf2fgenotype"
  val commandDescription: String = "Single-node VCF to ADAM converter (Note: the 'transform' command can take SAM or BAM as input)"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new Vcf2FlatGenotype(Args4j[Vcf2FlatGenotypeArgs](cmdLine))
  }
}

class Vcf2FlatGenotypeArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var bamFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-samtools_validation", usage = "SAM tools validation level")
  var validationStringency = SAMFileReader.ValidationStringency.LENIENT
  @Args4jOption(required = false, name = "-samples", usage = "Comma-separated set of samples to subset")
  var sampleSubset: String = null
  @Args4jOption(required = false, name = "-num_threads", usage = "Number of threads/partitions to use (default=4)")
  var numThreads = 4
  @Args4jOption(required = false, name = "-queue_size", usage = "Queue size (default = 10,000)")
  var qSize = 10000

  @Args4jOption(required = false, name = "-sample_block", usage = "The number of samples per parquet file")
  var sampleBlock = 100
}

class Vcf2FlatGenotype(args: Vcf2FlatGenotypeArgs) extends ADAMCommand {
  val companion = Vcf2FlatGenotype

  def run() = {

    // Quiet parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val sampleSubset: Option[Set[String]] =
      if (args.sampleSubset != null) {
        Some(args.sampleSubset.split(",").toSet)
      } else {
        None
      }

    val vcfReader = new VCFLineParser(new FileInputStream(new File(args.bamFile)), sampleSubset)

    val indexedSamples = vcfReader.samples.zipWithIndex.map {
      case (sample, i) => (i / args.sampleBlock, sample)
    }

    def createWriter(index: Int): AvroParquetWriter[ADAMFlatGenotype] =
      new AvroParquetWriter[ADAMFlatGenotype](
        new Path(args.outputPath + "/part_%d".format(index)),
        ADAMFlatGenotype.SCHEMA$,
        args.compressionCodec, args.blockSize, args.pageSize, !args.disableDictionary)

    val writers = indexedSamples.map(_._1).distinct.map(i => createWriter(i))

    val sampleWriters: Map[String, AvroParquetWriter[ADAMFlatGenotype]] =
      indexedSamples.map {
        case (i, sample) => (sample, writers(i))
      }.toMap

    var i: Long = 0
    var lineCount: Long = 0
    val million: Long = 1000000

    for (vcfLine <- vcfReader) {
      lineCount = 0

      VCFLineConverter.convert(vcfLine).foreach {
        case genotype: ADAMFlatGenotype => {
          sampleWriters(genotype.getSampleId.toString).write(genotype)
          lineCount += 1
        }
      }

      if (i / million < (i + lineCount) / million) {
        println("***** Read %d genotypes from VCF file *****".format(i + lineCount))
      }
      i += lineCount
    }

    vcfReader.close()
    writers.foreach(_.close())

    System.err.flush()
    System.out.flush()
    println("\nFinished! Converted %d lines total.".format(i))
  }
}

