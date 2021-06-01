package thoughtworks

import org.scalatest.{Ignore, Matchers}
import thoughtworks.NewYorkTimesAnalyzer._

class NewYorkTimesAnalyzerTest extends FeatureSpecWithSpark with Matchers {

  import spark.implicits._

  feature("count of records in books dataset") {
    scenario("calculate total number of books") {
      val columnName = "index"
      val testDF = Seq("book-1", "book-2", "book-3").toDF(columnName)

      val totalNumberOfBooks = testDF.totalQuantity(spark)

      totalNumberOfBooks should be(3)
    }
  }

  feature("average price of all books") {
    scenario("calculate the average price of all books") {
      val columnName = "nytprice"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val averagePriceOfBooks = testDF.averagePrice(spark)

      averagePriceOfBooks should be(2.0)
    }
  }

  feature("minimum price of books") {
    scenario("calculate the minimum price of all books") {
      val columnName = "nytprice"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val minPriceOfBooks = testDF.minimumPrice(spark)

      minPriceOfBooks should be(1.0)
    }
  }

  feature("maximum price of books") {
    scenario("calculate the maximum price of all books") {
      val columnName = "nytprice"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val maxPriceOfBooks = testDF.maximumPrice(spark)

      maxPriceOfBooks should be(3.0)
    }
  }

  feature("filter books published in a year") {
    scenario("calculate the total number of books published in a year") {
      val columnName = "publishedDate"
      val testDF = Seq("2008-01-05", "2009-03-05", "2008-02-09").toDF(columnName)

      val totalBooksPublishedInYear = testDF.totalBooksPublished(spark, "2008")

      totalBooksPublishedInYear should be(2)
    }
  }
}
