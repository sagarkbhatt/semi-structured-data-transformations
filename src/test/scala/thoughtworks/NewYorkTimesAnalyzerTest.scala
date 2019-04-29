package thoughtworks

import org.apache.spark.sql.types._
import org.scalatest.Matchers
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

  feature("transform published date of books") {
    scenario("transform published date of books into yyyy-MM-dd format") {

      val publishedDateField = StructField("published_date", StructType(
        List(
          StructField("$date", StructType(
            List(
              StructField("$numberLong", StringType, true)))
            , true)
        )
      ), true)
      val transformedPublishedDateField = StructField("publishedDate", DateType, true)

      val jsonStr =
        """[{"published_date":{"$date":{"$numberLong":"1212883200000"}}},
          |{"published_date":{"$date":{"$numberLong":"1213488000000"}}},
          |{"published_date":{"$date":{"$numberLong":"1212883200000"}}}
          |]""".stripMargin

      val testDF = spark.read
        .schema(StructType(
          List(
            publishedDateField
          )
        ))
        .json(Seq(jsonStr).toDS)
        .cache()

      val actualDF = testDF.transformPublishedDate(spark)

      val expectedJsonStr =
        """[{"published_date":{"$date":{"$numberLong":"1212883200000"}}, "publishedDate":"2008-06-08"},
          |{"published_date":{"$date":{"$numberLong":"1213488000000"}}, "publishedDate":"2008-06-15"},
          |{"published_date":{"$date":{"$numberLong":"1212883200000"}}, "publishedDate":"2008-06-08"}
          |]""".stripMargin

      val expectedDF = spark.read
        .schema(StructType(
          List(
            publishedDateField,
            transformedPublishedDateField
          )
        ))
        .json(Seq(expectedJsonStr).toDS)
        .cache()

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }

  feature("merge integer and double price of books into a single column") {
    scenario("simplify price column by merging integer and double price of books into a new column") {

      val priceField = StructField("price", StructType(
        List(
          StructField("$numberDouble", StringType, true),
          StructField("$numberInt", StringType, true)
        )
      ), true)
      val nytPriceField = StructField("nytprice", DoubleType, true)

      val jsonStr =
        """[{"price":{"$numberDouble":"24.95"}},
          |{"price":{"$numberInt":"27"}},
          |{"price":{"$numberInt":"22"}},
          |{"price":{"$numberDouble":"2.44"}}
          |]""".stripMargin

      val testDF = spark.read
          .schema(StructType(
            List(
              priceField
            )
          ))
        .json(Seq(jsonStr).toDS)
        .cache()

      val actualDF = testDF.mergeIntegerAndDoublePrice(spark)

      val expectedJsonStr =
        """[{"price":{"$numberDouble":"24.95"}, "nytprice":24.95},
          |{"price":{"$numberInt":"27"}, "nytprice":27.0},
          |{"price":{"$numberInt":"22"}, "nytprice":22.0},
          |{"price":{"$numberDouble":"2.44"}, "nytprice":2.44}
          |]""".stripMargin

      val expectedDF = spark.read
        .schema(StructType(
          List(
            priceField,
            nytPriceField
          )
        ))
        .json(Seq(expectedJsonStr).toDS)
        .cache()

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }
}
