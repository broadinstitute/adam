/*
 * Copyright (c) 2013. Regents of the University of California
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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.predicates.{ReadBucket, GBComparisons, BucketComparisons}
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._
import edu.berkeley.cs.amplab.adam.projections.{FieldValue, Projection}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRDD._
import edu.berkeley.cs.amplab.adam.util._
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{Option => Args4jOption, Argument}

import scala.collection._
import scala._
import scala.Some

import scala.collection.Seq
import java.util.logging.Level
import java.io.OutputStreamWriter

object CompareAdam extends AdamCommandCompanion with Serializable {

  val commandName: String = "compare"
  val commandDescription: String = "Compares two ADAM files"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new CompareAdam(Args4j[CompareAdamArgs](cmdLine))
  }

  def runGenerators(sc : SparkContext,
                    input1Path : String, input2Path : String,
                    generators : Seq[BucketComparisons]) : Map[BucketComparisons, Any] = {
    val schemas = generators.aggregate(Set[FieldValue](
      recordGroupId,
      readName,
      readMapped,
      primaryAlignment,
      readPaired,
      firstOfPair))(
        (s, b) => s ++ b.schemas, (s1, s2) => s1 ++ s2).toSeq
    val engine = new ComparisonTraversalEngine(schemas, input1Path, input2Path)(sc)
    Map(generators.map(g => (g, engine.generate(g))): _*)
  }
}

class ComparisonTraversalEngine(schema: Seq[FieldValue], input1Path : String, input2Path : String)(implicit sc : SparkContext) {
  lazy val projection = Projection(schema: _*)

  lazy val dict1 = sc.adamDictionaryLoad[ADAMRecord](input1Path)
  lazy val dict2 = sc.adamDictionaryLoad[ADAMRecord](input2Path)

  lazy val map = dict1.mapTo(dict2)

  lazy val reads1 : RDD[ADAMRecord] = sc.adamLoad(input1Path, projection=Some(projection))
  lazy val reads2 : RDD[ADAMRecord] = sc.adamLoad(input2Path, projection=Some(projection))
  lazy val remappedReads1 = reads1.remapReferenceId(map.toMap)

  lazy val named1 = reads1.adamSingleReadBuckets().map(ReadBucket.singleReadBucketToReadBucket).keyBy(_.allReads.head.getReadName)
  lazy val named2 = reads2.adamSingleReadBuckets().map(ReadBucket.singleReadBucketToReadBucket).keyBy(_.allReads.head.getReadName)

  lazy val joined = named1.join(named2).persist(StorageLevel.MEMORY_AND_DISK_SER)

  def generate(generator: BucketComparisons): Any = {
    joined.map {
      case (name, (bucket1, bucket2)) =>
        (name, generator.matchedByName(bucket1, bucket2))
    }.aggregate(generator.initialValue)((a, b) => generator.combine(a, b._2), (a, b) => generator.combine(a, b))
  }
}

trait GeneratorFilter[T] extends Serializable {
  def passesFilter(value : T) : Boolean
}

class RangeFilter(start : Long, end : Long) extends GeneratorFilter[Long] {
  def passesFilter(value: Long): Boolean =
    start <= value && value < end
}

case class ComparisonResult(total : Long, unique : Long, matching : Long) {}

class CompareAdamArgs extends Args4jBase with SparkArgs with ParquetArgs with Serializable {

  @Argument(required = true, metaVar = "INPUT1", usage = "The first ADAM file to compare", index = 0)
  val input1Path: String = null

  @Argument(required = true, metaVar = "INPUT2", usage = "The second ADAM file to compare", index = 1)
  val input2Path: String = null

  @Args4jOption(required = false, name = "-comparisons", usage = "Comma-separated list of comparisons to run")
  val comparisons: String = null

  @Args4jOption(required = false, name = "-list", usage = "Options to list, e.g. \"comparisons\" for a list of available comparisons")
  val list: String = null

  @Args4jOption(required = false, name = "-directory", usage = "Directory to generate the comparison output files")
  val directory: String = null
}

class CompareAdam(protected val args: CompareAdamArgs) extends AdamSparkCommand[CompareAdamArgs] with Serializable {
  val companion: AdamCommandCompanion = CompareAdam

  def run(context: SparkContext, job: Job): Unit = {

    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    if(args.list != null) {
      if(args.list == "comparisons") {
        println("\nAvailable comparisons:")
        GBComparisons.generators.foreach {
          generator =>
            println("\t%10s : %s".format(generator.name, generator.description))
        }
      }

      return
    }

    implicit def sc = context

    val generators: Seq[BucketComparisons] = args.comparisons match {
      case null => GBComparisons.generators
      case s =>
        val all = s.split(',')
        val map: Map[String, BucketComparisons] =
          GBComparisons.generators.foldLeft(Map[String, BucketComparisons]())((a: Map[String, BucketComparisons], b: BucketComparisons) => a + ((b.name, b)))
        all.map(k => map.getOrElse(k, throw new ArrayIndexOutOfBoundsException(String.format("Could not find generator %s", k))))
    }

    val values = CompareAdam.runGenerators(sc, args.input1Path, args.input2Path, generators)

    if (args.directory != null) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      values.foreach {
        case (gen, value) => {
          val output = new OutputStreamWriter(fs.create(new Path(args.directory, gen.name)))
          gen.write(value, output)

          output.close()
        }
      }
    } else {
      values.foreach {
        case (gen, value) =>
          println(gen.name)
          println(value)
      }
    }
  }

}

