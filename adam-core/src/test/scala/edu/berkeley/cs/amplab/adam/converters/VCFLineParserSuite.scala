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

import org.scalatest.FunSuite
import org.bdgenomics.adam.converters.{VCFLineConverter, VCFLineParser}

class VCFLineParserSuite extends FunSuite {

  test("VCFLineParser reads lines from bqsr1.vcf") {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("small.vcf")
    val parser = new VCFLineParser(is)
    val fgenotypes = parser.flatMap(VCFLineConverter.convert)
    assert(fgenotypes.length === 15)
  }


}
