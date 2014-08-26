/*
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.features

import java.io.File

import org.bdgenomics.formats.avro.{FeatureAttribute, Strand, Contig, Feature2}
import scala.collection.JavaConversions._

import scala.io.Source

trait Feature2Parser extends Serializable {
  def parse(line: String): Iterator[Feature2]
}

class Feature2File( parser : Feature2Parser ) extends Serializable {
  def parse( file : File ) : Iterator[Feature2] =
    Source.fromFile(file).getLines().flatMap { line =>
      parser.parse(line)
    }
}

/**
 * GTF is a line-based GFF variant.
 *
 * Details of the GTF/GFF format here:
 * http://www.ensembl.org/info/website/upload/gff.html
 */
class GTFParser2 extends Feature2Parser {

  private def mapAttributes( attrs : Map[String,String] ) : List[FeatureAttribute] =
    attrs.map {
      case (key : String, value : String) =>
        FeatureAttribute.newBuilder().setKey(key).setValue(value).build()
    }.toList

  override def parse(line: String): Seq[Feature2] = {
    // Just skip the '#' prefixed lines, these are comments in the
    // GTF file format.
    if (line.startsWith("#")) {
      return Seq()
    }

    val fields = line.split("\t")

    /*
    1. seqname - name of the chromosome or scaffold; chromosome names can be given with or without the 'chr' prefix. Important note: the seqname must be one used within Ensembl, i.e. a standard chromosome name or an Ensembl identifier such as a scaffold ID, without any additional content such as species or assembly. See the example GFF output below.
    2. source - name of the program that generated this feature, or the data source (database or project name)
    3. feature - feature type name, e.g. Gene, Variation, Similarity
    4. start - Start position of the feature, with sequence numbering starting at 1.
    5. end - End position of the feature, with sequence numbering starting at 1.
    6. score - A floating point value.
    7. strand - defined as + (forward) or - (reverse).
    8. frame - One of '0', '1' or '2'. '0' indicates that the first base of the feature is the first base of a codon, '1' that the second base is the first base of a codon, and so on..
    9. attribute - A semicolon-separated list of tag-value pairs, providing additional information about each feature.
     */

    val (seqname, source, feature, start, end, score, strand, frame, attribute) =
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))

    lazy val attrs = GTFParser.parseAttrs(attribute)

    val contig = Contig.newBuilder().setContigName(seqname).build()
    val f = Feature2.newBuilder()
      .setContig(contig)
      .setStart(start.toLong - 1) // GTF/GFF ranges are 1-based
      .setEnd(end.toLong) // GTF/GFF ranges are closed
      .setFeatureType(feature)
      .setSource(source)

    val _strand = strand match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    }
    f.setStrand(_strand)

    val (_id, _parentId) =
      feature match {
        case "gene"       => (Option(attrs("gene_id")), None)
        case "transcript" => (Option(attrs("transcript_id")), Option(attrs("gene_id")))
        case "exon"       => (Option(attrs("exon_id")), Option(attrs("transcript_id")))
        case _            => (attrs.get("id"), None)
      }
    _id.foreach(f.setFeatureId)
    _parentId.foreach(parentId => f.setParentIds(List[CharSequence](parentId)))

    f.setAttributes(mapAttributes(attrs))

    Seq(f.build())
  }
}



