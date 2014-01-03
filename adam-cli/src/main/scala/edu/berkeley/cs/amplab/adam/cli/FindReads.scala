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

import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import org.kohsuke.args4j.{Option => Args4jOption, Argument}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object FindReads extends AdamCommandCompanion {
  val commandName: String = "find_reads"
  val commandDescription: String = "Find reads that match particular individual or comparative criteria"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new FindReads(Args4j[FindReadsArgs](cmdLine))
  }
}

class FindReadsArgs extends Args4jBase with SparkArgs with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM sequence dictionary to print", index = 0)
  val inputPath: String = null

  @Args4jOption(name = "-readName", usage = "Read name to find and print")
  val readName: String = null
}

class FindReads(protected val args: FindReadsArgs) extends AdamSparkCommand[FindReadsArgs] with Serializable {
  val companion: AdamCommandCompanion = FindReads

  def run(sc: SparkContext, job: Job): Unit = {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val rdd: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)
    rdd.filter(r => r.getReadName == args.readName).collect().foreach(println(_))
  }

}
