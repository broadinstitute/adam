/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.rdd.compare

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.predicates.{MatchGenerator, GBGenerators}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

class CompareAdamSuite extends SparkFunSuite {

  sparkTest("Test that reads12.sam and reads21.sam are the same") {
    val reads12 = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val reads21 = ClassLoader.getSystemClassLoader.getResource("reads21.sam").getFile

    val dict1 = sc.adamDictionaryLoad[ADAMRecord](reads12)
    val dict2 = sc.adamDictionaryLoad[ADAMRecord](reads21)
    val map12 : collection.Map[Int,Int] = dict1.mapTo(dict2)

    val generators : Seq[MatchGenerator] = Seq(new GBGenerators.SameMappedPosition(map12.toMap))
    val values = CompareAdam.runGenerators(sc, reads12, reads21, generators)

    val sameValue = values(generators(0))
    assert( sameValue === 200 )
  }
}
