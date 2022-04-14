package spark.project
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    // conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    conf.set("spark.sql.catalog.spark_catalog.type", "hive")
    conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.local.type", "hadoop")
    conf.set("spark.sql.catalog.local.warehouse", "data/warehouse")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("SimpleApp")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val version = spark.version
    println(version)

    spark.sql("CREATE TABLE IF NOT EXISTS local.db.table (id bigint, data string) USING iceberg")

    spark.sql("INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c');")

    val df = spark.table("local.db.table")
    df.show()

    println(greeting())
  }

  def greeting(): String = "Hello, world!"
}
