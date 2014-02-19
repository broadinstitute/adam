/*
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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.models.{SequenceDictionary, ReferenceMapping, ReferenceRegion}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.Predef._
import org.apache.spark.SparkContext

class NonoverlappingReferenceRegions(seqDict: SequenceDictionary, regions: Seq[(Int, Seq[ReferenceRegion])])
  extends Serializable {
  assert(regions != null, "Regions was set to null")

  val regionMap = Map(regions.map(r => (r._1, new NonoverlappingRegions(seqDict, r._2))): _*)

  def regionsFor[U](regionable: U)(implicit mapping: ReferenceMapping[U]): Iterable[ReferenceRegion] =
    regionMap(mapping.getReferenceId(regionable)).regionsFor(regionable)
}

object NonoverlappingReferenceRegions {
  def apply[T](seqDict : SequenceDictionary, values : Seq[T])(implicit mapping: ReferenceMapping[T]):
    NonoverlappingReferenceRegions = {
    new NonoverlappingReferenceRegions(seqDict,
      values.map(v => (mapping.getReferenceId(v),mapping.getReferenceRegion(v)))
        .groupBy(t => t._1)
        .map(t => (t._1, t._2.map(k => k._2)))
        .toSeq)
  }
}

class NonoverlappingRegions(seqDict : SequenceDictionary, regions: Seq[ReferenceRegion]) extends Serializable {
  assert(regions != null, "Regions was set to null")
  assert(regions.head != null, "Regions must have at least one entry")

  val referenceId : Int = regions.head.refId

  // invariant: all the values in the 'regions' list have the same referenceId
  assert( !regions.exists(_.refId != referenceId) )

  val endpoints : Array[Long] =
    mergeRegions(regions.sortBy(r => r.start)).flatMap(r => Seq(r.start, r.end)).distinct.sorted.toArray

  assert( regions.size > 0 )

  val referenceLength : Long = seqDict(referenceId).length

  private def updateListWithRegion(list : List[ReferenceRegion], newRegion : ReferenceRegion) : List[ReferenceRegion] = {
    list match {
      case head :: tail =>
        if(head.overlaps(newRegion)) {
          head.hull(newRegion) :: tail
        } else {
          newRegion :: list
        }
      case _ => List(newRegion)
    }
  }

  def mergeRegions(regs : Seq[(ReferenceRegion)]) : List[ReferenceRegion] =
    regs.aggregate(List[ReferenceRegion]())(
      (lst: List[ReferenceRegion], p: (ReferenceRegion)) => updateListWithRegion(lst, p),
      (a, b) => a++b)

  def binaryPointSearch(pos : Long, lessThan : Boolean) : Int = {
    var i = 0
    var j = endpoints.size-1

    while(j-i > 1) {
      val ij2 = (i + j)/2
      val mid = endpoints(ij2)
      if(mid < pos) {
        i = ij2
      } else {
        j = ij2
      }
    }

    if(lessThan) i else j
  }

  def binaryRegionSearch(query : ReferenceRegion) : Seq[ReferenceRegion] = {
    if(query.end <= endpoints.head || query.start >= endpoints.last) {
      Seq()
    } else {
      val firsti = binaryPointSearch(query.start, lessThan=true)
      val lasti = binaryPointSearch(query.end, lessThan=false)

      // Slice is an inclusive start, exclusive end operation
      val startSlice = endpoints.slice(firsti, lasti)
      val endSlice = endpoints.slice(firsti + 1, lasti + 1)
      startSlice.zip(endSlice).map{
        case (start, end) =>
          ReferenceRegion(referenceId, start, end)
      }.toSeq
    }
  }

  def regionsFor[U](regionable: U)(implicit mapping : ReferenceMapping[U]): Iterable[ReferenceRegion] =
    binaryRegionSearch(mapping.getReferenceRegion(regionable))

  override def toString: String =
    "%d:%d-%d (%s)".format(referenceId, endpoints.head, endpoints.last, endpoints.mkString(","))
}

object NonoverlappingRegions {

  def apply[T](seqDict : SequenceDictionary, values : Seq[T])(implicit refMapping : ReferenceMapping[T]) =
    new NonoverlappingRegions(seqDict, values.map(value => refMapping.getReferenceRegion(value)))

}

/**
 * Original Join Design:
 *
 * Parameters:
 *   (1) f : (Range, Range) => T  // an aggregation function
 *   (2) a : RDD[Range]
 *   (3) b : RDD[Range]
 *
 * Return type: RDD[(Range,T)]
 *
 * Algorithm:
 *   1. a.collect() (where a is smaller than b)
 *   2. build a non-overlapping partition on a
 *   3. ak = a.map( v => (partition(v), v) )
 *   4. bk = b.flatMap( v => partitions(v).map( i=>(i,v) ) )
 *   5. joined = ak.join(bk).filter( (i, (r1, r2)) => r1.overlaps(r2) ).map( (i, (r1,r2))=>(r1, r2) )
 *   6. return: joined.reduceByKey(f)
 *
 * Ways in which we've generalized this plan:
 * - make it an aggregation, not a reduceByKey, in the last step
 * - figure out a way to not broadcast partition(s), once it's built
 * - carry along T,U with the Range values of each T and U.
 * - carry a sequence dictionary through the computation.
 **/

object Join {
  /**
   * This method does a join between different types which can have a corresponding ReferenceMapping.
   *
   * A first warning: this method contains a "collect" initially; the baseRDD needs to be pretty small in order to allow
   * for this to be handled properly.
   */
  def regionJoin[T, U, Value](sc: SparkContext,
                               seqDict : SequenceDictionary,
                               baseRDD: RDD[T],
                               joinedRDD: RDD[U],
                               zeroValue: Value,
                               seqOp: (Value, (T, U)) => Value,
                               combOp: (Value, Value) => Value)
                              (implicit tMapping: ReferenceMapping[T],
                               uMapping: ReferenceMapping[U],
                               tManifest: ClassManifest[T],
                               uManifest: ClassManifest[U],
                               valueManifest: ClassManifest[Value])
  : Value = {
    val regions = sc.broadcast(
      new NonoverlappingReferenceRegions(seqDict, baseRDD.map(t =>
        (tMapping.getReferenceId(t), tMapping.getReferenceRegion(t)))
          .groupBy(t => t._1)
          .map(t => (t._1, t._2.map(k => k._2)))
          .collect()
          .toSeq)
    )

    val smallerKeyed : RDD[(ReferenceRegion, T)] =
      baseRDD.keyBy(t => regions.value.regionsFor(t).head)

    val largerKeyed : RDD[(ReferenceRegion, U)] =
      joinedRDD.flatMap(t => regions.value.regionsFor(t).map((r : ReferenceRegion) => (r, t)))

    val joined: RDD[(ReferenceRegion, (T,U))] =
      smallerKeyed.join(largerKeyed)

    val filtered: RDD[(ReferenceRegion, (T, U))] = joined.filter({
      case (rr: ReferenceRegion, (t : T, u : U)) =>
        tMapping.getReferenceRegion(t).overlaps(uMapping.getReferenceRegion(u))
    })

    filtered.map(rrtu => rrtu._2).aggregate(zeroValue)(seqOp, combOp)
  }
}
