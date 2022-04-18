package app.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters._

trait AppSpark {
    val catalog_config = ConfigFactory.load("spark.conf").getConfig("spark.sql.catalog")
    val config_map = catalog_config
        .entrySet()
        .asScala
        .map { entry =>
            {"spark.sql.catalog." + entry.getKey} -> entry.getValue.render()
        }
        .toMap
    
    val spark_conf = new SparkConf()
    spark_conf.setAll(config_map)

    val spark = SparkSession
        .builder()
        .enableHiveSupport()
        .appName("SimpleApp")
        .master("local[*]")
        .config(spark_conf)
        .getOrCreate()
}