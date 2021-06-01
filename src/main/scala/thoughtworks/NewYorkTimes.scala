package thoughtworks

import org.apache.spark.sql.SparkSession
import thoughtworks.NewYorkTimesAnalyzer._

object NewYorkTimes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Analyze New York Times - Books Data Spark App").getOrCreate()

    val nytDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("./src/main/resources/nyt2.json")
      .cache()

    val nytTransformedDF = nytDF.mergeIntegerAndDoublePrice(spark)
      .transformPublishedDate(spark)

    val totalNumRows = nytTransformedDF.totalQuantity(spark)
    val averagePrice = nytTransformedDF.averagePrice(spark)
    val minimumPrice = nytTransformedDF.minimumPrice(spark)
    val maximumPrice = nytTransformedDF.maximumPrice(spark)
    val totalNumBooksPublished = nytTransformedDF.totalBooksPublished(spark, "2008")

    println("Initial Analysis of Books from New York Times Data shows: \n")
    println(s"The schema of the data is")
    nytDF.printSchema()
    nytDF.show()
    println(s"The total number of books we have data about is $totalNumRows")
    println(f"The average price of books sold is $averagePrice%1.2f")
    println(f"The minimum price of books sold is $minimumPrice%1.2f")
    println(f"The maximum price of books sold is $maximumPrice%1.2f")
    println(s"The number of books published in year 2008 are $totalNumBooksPublished")

    System.in.read()
    spark.stop()
  }
}
