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
package org.bdgenomics.adam.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import scala.reflect.ClassTag

class ParquetPartition() extends Partition {
}

class ParquetRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    ???
  }

  protected def getPartitions: Array[Partition] = Array()
}
