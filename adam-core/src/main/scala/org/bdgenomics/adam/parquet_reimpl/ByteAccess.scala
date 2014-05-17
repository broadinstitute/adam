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

import java.io._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import scala.Serializable

trait ByteAccess {
  def length(): Long
  def readByteStream(offset: Long, length: Int): InputStream

  def readFully(offset: Long, length: Int): Array[Byte] = {
    assert(length >= 0, "length %d should be non-negative".format(length))
    assert(offset >= 0, "offset %d should be non-negative".format(offset))
    var totalBytesRead: Int = 0
    val buffer = new Array[Byte](length)
    val is = readByteStream(offset, length)
    while (totalBytesRead < length) {
      val bytesRead = is.read(buffer, totalBytesRead, length - totalBytesRead)
      totalBytesRead += bytesRead
    }
    buffer
  }
}

class ByteArrayByteAccess(val bytes : Array[Byte]) extends ByteAccess with Serializable {
  override def length(): Long = bytes.length
  override def readByteStream(offset: Long, length: Int): InputStream = {
    val is = new ByteArrayInputStream(bytes)
    is.skip(length)
    is
  }
}

class InputStreamByteAccess(f: File) extends ByteAccess {

  override def length(): Long = f.length()

  override def readByteStream(offset: Long, length: Int): InputStream = {
    val fileIo = new FileInputStream(f)
    fileIo.skip(offset)
    new BoundedInputStream(fileIo, length.toLong)
  }

}

class BoundedInputStream(io: InputStream, len: Long) extends InputStream {

  var bytesRead: Long = 0

  override def read(): Int =
    if (bytesRead < len) {
      bytesRead += 1
      io.read()
    } else {
      throw new EOFException("read %d bytes out of maximum %d".format(bytesRead, len))
    }
}

class S3ByteAccess(client: AmazonS3, bucket: String, keyName: String) extends ByteAccess {
  assert(bucket != null)
  assert(keyName != null)

  lazy val objectMetadata = client.getObjectMetadata(bucket, keyName)
  override def length(): Long = objectMetadata.getContentLength
  override def readByteStream(offset: Long, length: Int): InputStream = {
    val getObjectRequest = new GetObjectRequest(bucket, keyName).withRange(offset, offset + length)
    client.getObject(getObjectRequest).getObjectContent
  }

}
