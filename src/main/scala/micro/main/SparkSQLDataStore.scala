package micro.main

import micro.main.SparkFactory.{filePath, loadAsTable, newSparkSession}
import org.apache.spark.sql.SparkSession

object SparkSQLDataStore {

  lazy val sparkSession: SparkSession = newSparkSession("sql-table", "local[4]")
 // lazy val sparkSession: SparkSession = newSparkSession("sql-table", "spark://192.168.0.106:7077")
  lazy val vehicleTable = loadAsTable(sparkSession, filePath("/annual-goods-vehicle-and-bus-population-by-make.csv"), "vehicle")

}
