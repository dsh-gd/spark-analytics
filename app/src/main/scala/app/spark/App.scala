package app.spark

object App extends AppSpark {
  def main(args: Array[String]): Unit = {

    val version = spark.version
    println("SPARK VERSION = " + version)

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

    val nRows: Long = df.count()
    val nCols: Long = df.columns.length

    println(s"\nNumber of rows: $nRows")
    println(s"Number of columns: $nCols\n")

    // stats_df.writeTo("local.db.simple_stats")
    //   .create()

    // val new_df = spark.table("local.db.simple_stats") 
    // new_df.show()
  }
}
