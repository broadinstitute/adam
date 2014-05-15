package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.FunSuite
import java.io.File

class ConfigurationFileSuite extends FunSuite {

  test("Can read values out of a file") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val config = new ConfigurationFile(file)
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties.contains("accessKey_s3"))
    assert(config.properties.contains("secretKey_s3"))
    assert(config.properties("accessKey") === "accessKey")
    assert(config.properties("secretKey") === "secretKey")
    assert(config.properties("accessKey_s3") === "accessKey_s3")
    assert(config.properties("secretKey_s3") === "secretKey_s3")
  }

  test("Reads default values when the file does not exist.") {
    val path = "/foo/bar.conf"
    val file = new File(path)
    val defaultMap = Seq("accessKey" -> "foo", "secretKey" -> "bar").toMap
    val config = new ConfigurationFile(file, Some(defaultMap))
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties("accessKey") === "foo")
    assert(config.properties("secretKey") === "bar")
  }

  test("Does not read the default values, when the file does exist.") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val defaultMap = Seq("accessKey" -> "foo", "secretKey" -> "bar").toMap
    val config = new ConfigurationFile(file, Some(defaultMap))
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties("accessKey") === "accessKey")
    assert(config.properties("secretKey") === "secretKey")
  }

}

class ConfigurationParserSuite extends FunSuite {
  test("parses a simple file") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val map: Map[String, String] = ConfigurationParser(file)

    assert(map.size === 4)

    assert(map.contains("accessKey"))
    assert(map.contains("secretKey"))
    assert(map("accessKey") === "accessKey")
    assert(map("secretKey") === "secretKey")

    assert(map.contains("accessKey_s3"))
    assert(map.contains("secretKey_s3"))
    assert(map("accessKey_s3") === "accessKey_s3")
    assert(map("secretKey_s3") === "secretKey_s3")
  }
}
