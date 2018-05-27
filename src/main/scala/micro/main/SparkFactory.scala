package micro.main

import java.nio.file.Paths

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkFactory {

  val _LOG = LoggerFactory.getLogger(this.getClass.getName)

  def newSparkSession(appName: String, master: String): SparkSession = {
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()

    _LOG.info("Context details {}", sparkSession)
    sparkSession

  }

  def loadAsTable(context: SparkSession, fileName: String, table: String): SparkSession = {
    import context.implicits._
    val data = context.read.format("csv").option("header", "true").csv(fileName).as[Vehicle]
    data.printSchema()
    _LOG.info("Schema details {}", data.schema.treeString)
    data.cache()
    data.createOrReplaceTempView(table)
    context
  }


  def filePath(filePath: String): String = {
    _LOG.info("Loading content from file {}", filePath)
    val fileUrl = this.getClass.getResource(filePath)
    Paths.get(fileUrl.toURI).toFile.getAbsolutePath
  }

}
