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
package edu.berkeley.cs.amplab.adam.rdd.compare

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.projections.Projection
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._

import scala.collection._
import scala.Some
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket
import edu.berkeley.cs.amplab.adam.predicates.{MatchGenerator, GBGenerators}

object CompareAdam extends Serializable {

  def apply(cmdLine: Array[String]): AdamCommand = {
    new CompareAdam(Args4j[CompareAdamArgs](cmdLine))
  }

  def runGenerators(sc : SparkContext,
                    input1Path : String, input2Path : String,
                    generators : Seq[MatchGenerator]) : Map[MatchGenerator, Any] = {

    val engine = new ComparisonTraversalEngine(input1Path, input2Path)(sc)
    Map(generators.map(g => (g, engine.generate(g))): _*)
  }

}

class ComparisonTraversalEngine(input1Path : String, input2Path : String)(implicit sc : SparkContext) {

  val projection = Projection(
    referenceId,
    mateReferenceId,
    readMapped,
    mateMapped,
    readPaired,
    firstOfPair,
    primaryAlignment,
    referenceName,
    mateReference,
    start,
    mateAlignmentStart,
    cigar,
    readNegativeStrand,
    mateNegativeStrand,
    readName)

  val reads1 : RDD[ADAMRecord] = sc.adamLoad(input1Path, projection=Some(projection))
  val reads2 : RDD[ADAMRecord] = sc.adamLoad(input2Path, projection=Some(projection))

  val dict1 = sc.adamDictionaryLoad[ADAMRecord](input1Path)
  val dict2 = sc.adamDictionaryLoad[ADAMRecord](input2Path)

  val named1 = reads1.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)
  val named2 = reads2.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)

  val joined = named1.join(named2)

  def generate(generator: MatchGenerator): Any = {
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

  @Argument(required = false, metaVar = "-generators", usage = "Generators to run for the comparison", index=2)
  val generators: String = null
}

class CompareAdam(protected val args: CompareAdamArgs) extends AdamSparkCommand[CompareAdamArgs] with Serializable {
  val companion: AdamCommandCompanion = CompareAdam

  def run(context: SparkContext, job: Job): Unit = {

    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    implicit def sc = context

    val generators: Seq[MatchGenerator] = args.generators match {
      case null => GBGenerators.generators
      case s => {
        val all = s.split(',')
        val map: Map[String, MatchGenerator] =
          GBGenerators.generators.foldLeft(Map[String, MatchGenerator]())((a: Map[String, MatchGenerator], b: MatchGenerator) => a + ((b.name, b)))
        all.map(k => map.getOrElse(k, throw new ArrayIndexOutOfBoundsException(String.format("Could not find generator %s", k))))
      }
    }

    val values = CompareAdam.runGenerators(sc, args.input1Path, args.input2Path, generators)

    values.foreach {
      case (gen, value) =>
        println(gen.name)
        println(value)
    }
  }

}

