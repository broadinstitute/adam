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
package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.FunSuite

class FileLocatorSuite extends FunSuite {

  test("FileLocator.parseSlash can correctly parse a one-slash string") {
    assert(FileLocator.parseSlash("foo/bar") === Some(("foo", "bar")))
    assert(FileLocator.parseSlash("/foo") === Some(("", "foo")))
  }

  test("FileLocator.parseSlash can correctly parse a two-slash string") {
    assert(FileLocator.parseSlash("foo/bar") === Some(("foo", "bar")))
    assert(FileLocator.parseSlash("/foo/bar") === Some(("/foo", "bar")))
  }

  test("FileLocator.parseSlash can correctly parse a no-slash string") {
    assert(FileLocator.parseSlash("foo") === None)
  }
}
