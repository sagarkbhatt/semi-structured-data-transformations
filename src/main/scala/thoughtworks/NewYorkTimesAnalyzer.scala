package thoughtworks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object NewYorkTimesAnalyzer {

  implicit class NYTDataframe(val nytDF: Dataset[Row]) {

    def totalQuantity(spark: SparkSession): Long = {
      nytDF.count()
    }

    def mergeIntegerAndDoublePrice(spark: SparkSession): Dataset[Row] = {
      import spark.implicits._

      val map = Map("doublePrice" -> 0.0, "intPrice" -> 0.0)
      val withNytPriceDF = nytDF
        .withColumn("doublePrice", nytDF.col("price.$numberDouble").cast("double"))
        .withColumn("intPrice", nytDF.col("price.$numberInt").cast("double"))
        .na
        .fill(map)
        .withColumn("nytprice", $"doublePrice" + $"intPrice")

      withNytPriceDF
        .drop("doublePrice")
        .drop("intPrice")
    }

    def transformPublishedDate(spark: SparkSession): Dataset[Row] = {
      nytDF.withColumn("publishedDate",
        nytDF.col("published_date.$date.$numberLong")./(1000)
          .cast(DataTypes.TimestampType)
          .cast(DataTypes.DateType)
      )
    }

    def averagePrice(spark: SparkSession): Double = {
      import spark.implicits._

      val dataset: Dataset[Double] = nytDF.select(avg("nytprice")).as[Double]

      dataset.collect()(0)
    }

    def minimumPrice(spark: SparkSession): Double = {
      import spark.implicits._

      val dataset: Dataset[Double] = nytDF.select(min("nytprice")).as[Double]

      dataset.collect()(0)
    }

    def maximumPrice(spark: SparkSession): Double = {
      import spark.implicits._

      val dataset: Dataset[Double] = nytDF.select(max("nytprice")).as[Double]

      dataset.collect()(0)
    }

    def totalBooksPublished(spark: SparkSession, inYear: String): Long = {
      import spark.implicits._

      val isSoldInYear = year($"publishedDate") === inYear

      nytDF.select($"publishedDate")
        .filter(isSoldInYear)
        .count
    }
  }
}
