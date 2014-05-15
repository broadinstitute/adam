package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.FunSuite
import java.io.File

class CredentialsPropertiesTestSuite extends FunSuite {

  test("Can parse a simple configuration file with CredentialsProperties") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val cp = new CredentialsProperties(file)

    val aws = cp.awsCredentials()
    assert(aws.getAWSAccessKeyId === "accessKey")
    assert(aws.getAWSSecretKey === "secretKey")

    val aws_s3 = cp.awsCredentials(Some("s3"))
    assert(aws_s3.getAWSAccessKeyId === "accessKey_s3")
    assert(aws_s3.getAWSSecretKey === "secretKey_s3")

  }

}
