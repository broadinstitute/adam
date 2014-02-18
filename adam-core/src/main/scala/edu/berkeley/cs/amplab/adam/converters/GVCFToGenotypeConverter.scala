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
package edu.berkeley.cs.amplab.adam.converters

import java.io.InputStream
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.adam.avro.GBGenotype
import scala.io.Source
import org.apache.spark.rdd.RDD
import scala.math._

import scala.collection.JavaConversions._

class GVCFToGenotypeConverter(is : InputStream) {

  private val lines = Source.fromInputStream(is).getLines().withFilter(!_.startsWith("##"))
  private val header = lines.next().split("\t")
  private val samples = header.slice(9, header.size)

  def parse() : Iterator[GBGenotype] = {
    lines.flatMap(GVCFLineConverter.convert(samples, _))
  }

}

object GVCFToGenotypeConverter {

  implicit def convert(is : InputStream)(implicit sc : SparkContext) : RDD[GBGenotype] =
    sc.parallelize(new GVCFToGenotypeConverter(is).parse().toSeq)
}

object GVCFLineConverter {

  def unphred(score : Int) : Double = pow(10.0, -score)

  def plIndices(numAlt : Int) : Seq[(Int,Int)] = {
    (0 until numAlt+1).flatMap {
      j => (0 until j+1).map(i => (i, j))
    }
  }

  def convert(samples : Array[String], line : String) : Seq[GBGenotype] = {

    val fields = line.split("\t")
    val siteFields = fields.slice(0, 9)
    val sampleColumns = fields.slice(9, fields.size)

    val refAllele = siteFields(3)
    val altAlleles = siteFields(4).split(",")
    val info = siteFields(7).split(";").map(_.split("=")).map(arr => (arr(0), arr(1))).toMap

    val nonRefAltAllele = altAlleles.size == 1 && altAlleles(0) == "<NON_REF>"
    val blockSize = if(nonRefAltAllele) info("BLOCK_SIZE").toInt else refAllele.length
    val filteredReadDepth : Int = if(!nonRefAltAllele) info("DP").toInt else null.asInstanceOf[Int]

    val alleles = refAllele :: altAlleles.toList

    val sampleFieldNames = siteFields(8).split(":")

    val base = GBGenotype.newBuilder()
      .setReferenceName(siteFields(0))
      .setStart(siteFields(1).toLong)
      .setReferenceAllele(refAllele)
      .setLength(blockSize)
      .setFilteredReadDepth(filteredReadDepth).build()

    samples.zip(sampleColumns).flatMap {
      case (sample, column) => {
        val fields : Map[String,String] = sampleFieldNames.zip(column.split(":")).toMap
        val pls = fields("PL").split(",").map(_.toInt)

        val sampleBase = GBGenotype.newBuilder(base)
          .setSampleId(sample.hashCode)
          .setSampleName(sample)
          .setUnfilteredReadDepth(fields("DP").toInt).build()

        pls.zip(plIndices(altAlleles.size)).map {
          case (pl, (i, j)) =>
            GBGenotype.newBuilder(sampleBase)
              .setGenotype(List(alleles(i), alleles(j)))
              .setLikelihood(pl)
              .build()
        }
      }
    }
  }

}
