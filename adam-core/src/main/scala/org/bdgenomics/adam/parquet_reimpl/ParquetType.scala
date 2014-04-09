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

import parquet.schema._
import scala.collection.JavaConversions._
import parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * This is a reimplementation of the Type / GroupType / PrimitiveType / MessageType hierarchy
 * from parquet.schema, mostly driven by (SIGH) the non-Serializable, final nature of the
 * MessageType class.  #OMGWTF
 */
trait ParquetType extends Serializable {

  def name: String
  def repetition: Repetition.Value
  def paths(): Seq[TypePath]
  def lookup(path: TypePath): Option[ParquetType]
  def convertToParquet(): Type
}

object ParquetType {

  def convertFromType(pType: Type): ParquetType = {
    pType match {
      case primitive: PrimitiveType => new ParquetPrimitiveType(primitive)
      case schema: MessageType      => new ParquetSchemaType(schema)
      case group: GroupType         => new ParquetGroupType(group)
    }
  }

}

object Repetition extends Enumeration {
  val OPTIONAL, REQUIRED, REPEATED = Value

  def toParquetRepetition(rep: Value): Type.Repetition = Type.Repetition.valueOf(rep.toString)

  def apply(rep: Type.Repetition): Value = withName(rep.toString)
}

object TypePath {
  def apply(path: Seq[String]): TypePath = {
    if (path.length == 0) {
      throw new IllegalArgumentException()
    } else if (path.length == 1) {
      new TypePath(path(0))
    } else {
      new TypePath(path(0), TypePath(path.slice(1, path.length)))
    }
  }
}

case class TypePath(head: String, suffix: Option[TypePath]) {
  def this(head: String, rest: TypePath) = this(head, Some(rest))
  def this(head: String) = this(head, None)

  def length(): Int = suffix.map(_.length()).getOrElse(0) + 1
  override def toString = "%s%s".format(head, suffix.map(suff => ".%s".format(suff.toString)).getOrElse(""))
}

object PrimitiveType extends Enumeration {
  val INT32, INT64, INT96, BOOLEAN, DOUBLE, FLOAT, BINARY, FIXED_LEN_BYTE_ARRAY = Value

  def toParquetPrimitiveType(typ: Value): PrimitiveTypeName = PrimitiveTypeName.valueOf(typ.toString)

  def apply(typ: PrimitiveTypeName): Value = withName(typ.toString)
}

case class ParquetPrimitiveType(name: String, repetition: Repetition.Value, length: Option[Int], primitiveType: PrimitiveType.Value) extends ParquetType {
  def this(pType: PrimitiveType) = this(pType.getName, Repetition(pType.getRepetition), Option(pType.getTypeLength), PrimitiveType(pType.getPrimitiveTypeName))
  def paths(): Seq[TypePath] = Seq(new TypePath(name))
  def lookup(path: TypePath): Option[ParquetType] =
    if (path.suffix.isDefined) {
      None
    } else if (path.head != name) {
      None
    } else {
      Some(this)
    }

  def convertToParquet(): PrimitiveType = {
    val pRep = Type.Repetition.valueOf(repetition.toString)
    val pType = PrimitiveTypeName.valueOf(primitiveType.toString)
    new PrimitiveType(pRep, pType, length.getOrElse(0), name)
  }
}

abstract class ParquetAggregateType(val name: String, val repetition: Repetition.Value, val fields: Seq[ParquetType]) extends ParquetType {

  private val fieldMap: Map[String, ParquetType] = fields.map(field => field.name -> field).toMap

  def paths(): Seq[TypePath] = {
    fields.flatMap {
      field =>
        field.paths().map {
          path => new TypePath(name, path)
        }
    }
  }

  def lookup(path: TypePath): Option[ParquetType] =
    if (path.head != name) {
      None
    } else
      path.suffix match {
        case None         => Some(this)
        case Some(suffix) => fieldMap.get(suffix.head).flatMap(_.lookup(suffix))
      }

  def convertToParquet(): GroupType = {
    val pRep = Type.Repetition.valueOf(repetition.toString)
    new GroupType(pRep, name, fields.map(_.convertToParquet()).toList)
  }
}

case class ParquetGroupType(groupName: String, groupRepetition: Repetition.Value, groupFields: Seq[ParquetType]) extends ParquetAggregateType(groupName, groupRepetition, groupFields) {

  def this(pType: GroupType) = this(pType.getName, Repetition(pType.getRepetition), pType.getFields.map(f => ParquetType.convertFromType(f)))
}

case class ParquetSchemaType(schemaName: String, schemaFields: Seq[ParquetType]) extends ParquetAggregateType(schemaName, Repetition.REPEATED, schemaFields) {

  def this(pType: MessageType) = this(pType.getName, pType.getFields.map(f => ParquetType.convertFromType(f)))

  override def paths(): Seq[TypePath] = fields.flatMap(_.paths())

  override def convertToParquet(): MessageType = {
    new MessageType(name, fields.map(_.convertToParquet()).toList)
  }
}
