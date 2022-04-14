package spark.project
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    val version = spark.version
    println(version)
    
    println(greeting())
  }

  def greeting(): String = "Hello, world!"
}
