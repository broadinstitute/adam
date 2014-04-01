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
import org.kohsuke.args4j.{Option => Args4jOption, Argument}
import net.sf.samtools._
import edu.berkeley.cs.amplab.adam.avro.{ADAMFlatGenotype, ADAMRecord}
import scala.collection.JavaConversions._
import java.io.{FileInputStream, File}
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import java.util.concurrent._
import scala.Some
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.converters.{VCFLineParser, VCFLineConverter, VCFLine, SAMRecordConverter}

object Vcf2FlatGenotype extends AdamCommandCompanion {
  val commandName: String = "vcf2fgenotype"
  val commandDescription: String = "Single-node VCF to ADAM converter (Note: the 'transform' command can take SAM or BAM as input)"

  def apply(cmdLine: Array[String]): AdamCommand = {
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
  @Args4jOption(required = false, name = "-num_threads", usage = "Number of threads/partitions to use (default=4)")
  var numThreads = 4
  @Args4jOption(required = false, name = "-queue_size", usage = "Queue size (default = 10,000)")
  var qSize = 10000
}

class Vcf2FlatGenotype(args: Vcf2FlatGenotypeArgs) extends AdamCommand {
  val companion = Vcf2FlatGenotype

  def run() = {

    // Quiet parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val vcfReader = new VCFLineParser(new FileInputStream(new File(args.bamFile)))

    val parquetWriter = new AvroParquetWriter[ADAMFlatGenotype](
      new Path(args.outputPath),
      ADAMFlatGenotype.SCHEMA$,
      args.compressionCodec, args.blockSize, args.pageSize, !args.disableDictionary)

    var i = 0
    for (vcfLine <- vcfReader) {
      i += 1

      VCFLineConverter.convert(vcfLine).foreach(parquetWriter.write)

      if (i % 1000000 == 0) {
        println("***** Read %d million lines from VCF file (queue=%d) *****".format(i / 1000000))
      }
    }

    parquetWriter.close()
    vcfReader.close()

    System.err.flush()
    System.out.flush()
    println("\nFinished! Converted %d lines total.".format(i))
  }
}

