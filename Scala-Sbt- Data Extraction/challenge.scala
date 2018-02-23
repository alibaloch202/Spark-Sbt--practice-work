package example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame,SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark._
import java.nio.file.{Files, Paths}

object Hello extends fileImport with App 
{
  // Greetings
  println(greeting)
  // Start Spark Session
  val spark = SparkSession.builder().master("local").appName("TV Series Analysis").config("spark.sql.warehouse.dir","file:///tmp/spark-warehouse").getOrCreate()
  import spark.implicits._
  var anime = spark.read.option("header", true).csv("src/test/resources/data/anime.csv")
  var ratings = spark.read.option("header", true).csv("src/test/resources/data/rating.csv")
  /*
	Top 10 Ratings 
	anime.select("*").orderBy($"rating".desc).limit(10).show()
*/

/*
	First filter on all TV Series 
	anime.select($"rating").filter("type == 'TV'").orderBy($"rating".desc).limit(10).show()
*/


/*
	Filter on all TV Series and Episodes > 10
	Top 10 ratings
	
	anime.select("*").filter("type == 'TV' AND episodes > 10 ").orderBy($"rating".desc).limit(10).show()
*/


/* 
  Filter on all TV Series and Episodes > 10
	GroupBy based on GENRE, Aggregate ratings average
	Top 10 ratings

  Final Query
*/
  var top10TvSeries = anime.filter("type =='TV' AND episodes > 10")
                        .groupBy($"genre").agg(avg("rating").as("averageRating"))
                        .orderBy($"averageRating".desc)
                        .limit(10)
  top10TvSeries.show()
  top10TvSeries.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("src/test/resources/data/output")

/////////////////////////// Data Join  ///////////////////////////

// Join of Rating.csv and Anime.csv 
/*
  val output = anime.join(ratings,Seq("anime_id"),joinType="inner")

  val joinedtop10TvSeries = output.filter("type =='TV' AND episodes > 10")
                      .groupBy($"genre").agg(avg("rating").as("averageRating"))
                      .orderBy($"averageRating".desc)
                      .limit(10)

  output.show(10)
*/
/////////////////////////////////////////////////////////////////////

/*
  Scala Unit Testsd

*/

  // Test Number of Rows and check output directory
  def testNumberofDistinctRows(): (Long, Boolean) = 
  { 
    val spark = SparkSession.builder().master("local").appName("TV Series Analysis").config("spark.sql.warehouse.dir","file:///tmp/spark-warehouse").getOrCreate()
    import spark.implicits._
    var top10TvSeries = (spark.read
                .option("header", true).csv("src/test/resources/data/anime.csv"))
                .filter("type =='TV' AND episodes > 10")
                .groupBy($"genre").agg(avg("rating").as("averageRating"))
                .orderBy($"averageRating".desc)
                .limit(10)
    
    // Read output file for unit test

    val output = (spark.read.option("header", true).csv("src/test/resources/data/output/*.csv")).select("*")
    //val output = output_file.select("*")
    return (top10TvSeries.distinct.count, if ((top10TvSeries.except(output)).count==0) true else false)
  }
  // Directory Check
  def Output_Directory_Check : Boolean = 
  {
      var output_files = "src/test/resources/data/output/"
      var s= Files.exists(Paths.get(output_files))
      //println("Yes it is ", s , "Files Exists in directory")
      return s;
  }
  //set new runtime options
  //spark.conf.set("spark.sql.shuffle.partitions", 6)
  //spark.conf.set("spark.executor.memory", "2g")
}

trait fileImport {
  lazy val greeting: String = "hello"
}
