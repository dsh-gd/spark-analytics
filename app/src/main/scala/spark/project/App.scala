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

    println("Enter name of the dataset: ")

    var input = scala.io.StdIn.readLine()
    val dset_fname = "/" + input;
    var dset_path = ""

    try {      
      dset_path = getClass.getResource(dset_fname).getPath()
    } catch {
      case e: NullPointerException => {
        println(s"Error: File $input not found")
        System.exit(0)
      }
    }

    val df = spark.read
      .option("header", "true")
      .csv(dset_path)

    df.show()

    // stats_df.writeTo("local.db.simple_stats")
    //   .create()

    // val new_df = spark.table("local.db.simple_stats") 
    // new_df.show()
  }
}
