package thoughtworks

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import thoughtworks.AnalyzerUtils._

object Analyzer {

  implicit class NYTDataframe(val nytDF: Dataset[Row]) {

    def totalBooks(spark: SparkSession): Long = {
      nytDF.countRows(spark)
    }

    //Merge integer and double price of books into a single column
    def mergeIntegerAndDoublePrice(spark: SparkSession): Dataset[Row] = {
      import spark.implicits._
      val map = Map("doublePrice" -> 0.0, "intPrice" -> 0.0)
      val withNytPriceDF = nytDF
        .addAColumn(spark,"doublePrice", nytDF.col("price.$numberDouble").cast("double"))
        .addAColumn(spark,"intPrice", nytDF.col("price.$numberInt").cast("double"))
        .na
        .fill(map)
        .addAColumn(spark,"nytprice", $"doublePrice" + $"intPrice")

      withNytPriceDF
        .dropAColumn(spark, "doublePrice")
        .dropAColumn(spark, "intPrice")
    }

    //Transform published_date column into readable format
    def transformPublishedDate(spark: SparkSession): Dataset[Row] = {
      spark.emptyDataFrame
    }

    //Calculate average price of all books
    def averagePrice(spark: SparkSession): Double = {
      0.0
    }

    //Calculate minimum price of all books
    def minimumPrice(spark: SparkSession): Double = {
      0.0
    }

    //Calculate maximum price of all books
    def maximumPrice(spark: SparkSession): Double = {
      0.0
    }

    //Calculate total number of books published in a year
    def totalBooksPublished(spark: SparkSession, inYear: String): Long = {
      0
    }
  }
}
