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

import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.avro.ADAMVariant
import java.io._
import scala.io.Source
import org.apache.spark.rdd.RDD

/**
 * NOT TO BE CHECKED IN TO ADAM WITHOUT FURTHER REVIEW.
 */
object ADAMVariantConverter {

  def convertVCF(input : RDD[String]) : (SequenceDictionary, Iterable[ADAMVariant]) = {

    def seqOp(pair : (SequenceDictionary,Iterable[ADAMVariant]), line : String) = {
      val (newDict, newLines) = convert(pair._1, line)
      (newDict, pair._2 ++ newLines)
    }

    def combOp(p1 : (SequenceDictionary,Iterable[ADAMVariant]), p2 : (SequenceDictionary,Iterable[ADAMVariant])) = {
      (p1._1 ++ p2._1, p1._2 ++ p2._2)
    }

    input.filter(!_.startsWith("#"))
      .aggregate[(SequenceDictionary, Iterable[ADAMVariant])](
      (new SequenceDictionary(Seq()), Seq()))(seqOp, combOp)
  }

  def convert(baseDict : SequenceDictionary, vcfLine : String) : (SequenceDictionary, Iterable[ADAMVariant]) = {

    var seqDict = baseDict

    // #CHROM POS ID REF ALT QUAL FILTER INFO FORMAT SAMPLE*
    val base = ADAMVariant.newBuilder()

    val array : Array[String] = vcfLine.split("\t")
    val vcfChrom = array(0)
    val vcfPos = array(1)
    val vcfId = array(2)
    val vcfRef = array(3)
    val vcfAlt = array(4)
    val vcfQual = array(5)
    val vcfFilter = array(6)

    if(!seqDict.containsRefName(vcfChrom)) {
      val referenceName : String = vcfChrom
      val referenceId : Int = referenceName.hashCode()
      seqDict = seqDict + SequenceRecord(referenceId, referenceName, 1L, null)
    }

    base.setReferenceName(vcfChrom)
    base.setReferenceId(seqDict.apply(vcfChrom).id)
    base.setReferenceAllele(vcfRef)
    base.setPosition(vcfPos.toLong)
    if(vcfId != ".") { base.setId(vcfId) }
    if(vcfQual != ".") { base.setQuality(vcfQual.toInt) }
    if(vcfFilter != ".") { base.setFilters(vcfFilter) }

    val baseRow = base.build()

    def buildVariant(allele : String) : ADAMVariant = {
      ADAMVariant.newBuilder(baseRow)
        .setVariant(allele)
        .setIsReference(allele == vcfRef).build()
    }

    (seqDict, vcfAlt.split(",").filter(_ != ".").map(buildVariant) ++ Seq(buildVariant(vcfRef)))
  }
}
