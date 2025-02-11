package app.spark
import org.apache.spark.sql.functions._

object App extends AppSpark {
  def main(args: Array[String]): Unit = {

    val version = spark.version
    println("SPARK VERSION = " + version)

    println("Enter name of the dataset: ")

    var input = scala.io.StdIn.readLine()
    val dsetName = "/" + input;
    var dsetPath = ""

    try {      
      dsetPath = getClass.getResource(dsetName).getPath()
    } catch {
      case e: NullPointerException => {
        println(s"Error: File $input not found")
        System.exit(0)
      }
    }

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dsetPath)

    if (input == "titanic_data.csv") {
      // Number of passangers
      val countPassangers = df.count()
      println(countPassangers)

      // Number of survived passangers
      val countSurvived = df.groupBy("Survived").count()

      // Survival rate by sex
      val survivalRateSex = df.groupBy("Sex", "Survived").count()

      // Survival rate by class
      val survivalRateClass = df.groupBy("Pclass", "Survived").count()

      spark.sql("CREATE TABLE IF NOT EXISTS local.db.number_of_survived (Survived int, count bigint) USING iceberg")
      countSurvived.writeTo("local.db.number_of_survived").append()

      spark.sql("CREATE TABLE IF NOT EXISTS local.db.survival_rate_sex (Sex string, Survived int, count bigint) USING iceberg")
      survivalRateSex.createOrReplaceTempView("survivalRateSex")
      spark.sql("INSERT INTO local.db.survival_rate_sex SELECT * FROM survivalRateSex")

      spark.sql("CREATE TABLE IF NOT EXISTS local.db.survival_rate_class (Pclass string, Survived int, count bigint) USING iceberg")
      survivalRateClass.writeTo("local.db.survival_rate_class").append()

    } else if (input == "imdb_data.csv") {
      // Number of movies
      val countMovies = df.count()
      println(countMovies)

      // Number of movies by year
      val countMoviesByYear = df.groupBy(col("Released_Year").alias("year")).count()

      // Top 10 movies with highest rating
      val top10Movies = df.orderBy(desc("IMDB_Rating"))
        .limit(10)
        .select(
          col("Series_Title").alias("title"),
          col("Released_Year").alias("year"),
          col("IMDB_Rating").alias("rating")
        )

      spark.sql("CREATE TABLE IF NOT EXISTS local.db.number_of_movies_year (year string, count bigint) USING iceberg")
      countMoviesByYear.writeTo("local.db.number_of_movies_year").append()

      spark.sql("CREATE TABLE IF NOT EXISTS local.db.top_10_movies (title string, year string, rating double) USING iceberg")
      top10Movies.createOrReplaceTempView("top10movies")
      spark.sql("INSERT INTO local.db.top_10_movies SELECT * FROM top10movies")

    } else {
      println("Can't do any aggregation on this dataset.")
      System.exit(0)
    }
  }
}
