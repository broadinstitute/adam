package org.bdgenomics.adam.parquet_reimpl

import java.io.{ FileInputStream, File }
import org.apache.spark.Logging
import scala.io.Source
import com.amazonaws.auth.AWSCredentials

class CredentialsProperties(location: File) extends Serializable with Logging {
  def this() = this(new File("/etc/spark.conf"))

  val defaultAccessKey = System.getenv("AWS_ACCESS_KEY_ID")
  val defaultSecretKey = System.getenv("AWS_SECRET_KEY")
  private val defaultMap = Seq("accessKey" -> defaultAccessKey, "secretKey" -> defaultSecretKey).toMap

  logWarning("Default AWS Access Key from ENV: \"%s\"".format(defaultAccessKey))

  val configuration = new ConfigurationFile(location, Some(defaultMap))

  logWarning("AWS Access Key from /etc/spark.conf: \"%s\"".format(accessKey(None)))
  logWarning("S3 AWS Access Key from /etc/spark.conf: \"%s\"".format(accessKey(Some("s3"))))

  def configuredValue(keyName: String, suffix: Option[String] = None): String = {
    suffix match {
      case None => configuration.properties(keyName)
      case Some(suffixString) => {
        val combinedKey = "%s_%s".format(keyName, suffixString)
        if (configuration.properties.contains(combinedKey)) {
          logInfo("Found value for combined key \"%s\"".format(combinedKey))
          configuration.properties(combinedKey)
        } else {
          logInfo("No value for combined key \"%s\", returning value for default key \"%s\"".format(combinedKey, keyName))
          configuration.properties(keyName)
        }
      }
    }
  }

  def accessKey(suffix: Option[String]): String = configuredValue("accessKey", suffix)
  def secretKey(suffix: Option[String]): String = configuredValue("secretKey", suffix)

  def awsCredentials(suffix: Option[String] = None): AWSCredentials = {
    new SerializableAWSCredentials(accessKey(suffix), secretKey(suffix))
  }
}

private[parquet_reimpl] case class ConfigurationFile(properties: Map[String, String]) extends Serializable {
  def this(f: File, defaultValues: Option[Map[String, String]] = None) = this(ConfigurationParser(f, defaultValues))
}

private[parquet_reimpl] object ConfigurationParser extends Logging {

  def apply(f: File, defaultValues: Option[Map[String, String]] = None): Map[String, String] = {
    if (!f.exists() || !f.canRead) {
      logWarning("File \"%s\" does not exist, using default values.".format(f.getAbsolutePath))
      defaultValues.get

    } else {
      logInfo("Reading configuration values from \"%s\"".format(f.getAbsolutePath))
      val is = new FileInputStream(f)
      val lines = Source.fromInputStream(is).getLines().map(_.trim)
      val nonComments = lines.filter(line => !line.startsWith("#") && line.contains("="))
      val map = nonComments.map(_.split("=")).map(array => array.map(_.trim)).map(array => (array(0), array(1))).toMap
      is.close()
      map
    }
  }
}