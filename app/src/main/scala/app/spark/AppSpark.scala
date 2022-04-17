package app.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait AppSpark {
    val spark_conf = new SparkConf()

    // conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    spark_conf.set("spark.sql.catalog.spark_catalog.type", "hive")
    spark_conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    spark_conf.set("spark.sql.catalog.local.type", "hadoop")
    spark_conf.set("spark.sql.catalog.local.warehouse", "data/warehouse")

    val spark = SparkSession
        .builder()
        .enableHiveSupport()
        .appName("SimpleApp")
        .master("local[*]")
        .config(spark_conf)
        .getOrCreate()
}
