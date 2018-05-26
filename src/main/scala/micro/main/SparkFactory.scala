package micro.main

import java.nio.file.Paths

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkFactory {

   val _LOG = LoggerFactory.getLogger(this.getClass.getName)

  def newSparkSession(appName: String, master: String) = {
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()
    sparkSession
  }

  def loadAsTable(context: SparkSession, fileName: String, table: String): SparkSession = {
    import context.implicits._
    val data = context.read.format("csv").option("header", "true").csv(fileName).as[Vehicle]
    data.printSchema()
    data.cache()
    data.createOrReplaceTempView(table)
    context
  }


  def filePath(filePath: String): String = {
    val url = this.getClass.getResource("/logback.xml")
    _LOG.info("Path {}" , url)
    val fileUrl = this.getClass.getResource(filePath)
    Paths.get(fileUrl.toURI).toFile.getAbsolutePath
  }

}
