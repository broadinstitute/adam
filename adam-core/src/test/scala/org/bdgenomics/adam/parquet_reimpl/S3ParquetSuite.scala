package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.{ BeforeAndAfter, FunSuite }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3Client }
import parquet.hadoop.metadata.{ BlockMetaData, ParquetMetadata }
import collection.JavaConversions._

// class S3ParquetSuite extends FunSuite with BeforeAndAfter {
//   var client: AmazonS3 = null
//   var reader: S3ParquetFooterReader = null
//   var footer: ParquetMetadata = null
//   var blocks: Seq[BlockMetaData] = null

//   before {
//     client = new AmazonS3Client()
//     reader = new S3ParquetFooterReader(client, "genomebridge-variantstore-ci", "data/flannick/chr10.adam")
//     footer = reader.readFooter()
//     blocks = footer.getBlocks
//   }
// }
