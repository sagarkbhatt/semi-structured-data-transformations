### semi-structured-data-transformations
This repository has been built to help people learn how to work with semi-structured data sources with Spark+Scala.

##### Analyze New York Times - Books dataset and write code for below operations:
- count number of records
- merge integer and double values in price column into new column
- transform published date into readable format 
- calculate average price
- calculate min and max price
- count number of books published in a year

##### Goal: Do code changes for above operations and make sure all the test cases pass.

Dataset: src/main/resources/nyt2.json

Metadata: (Ref: https://www.kaggle.com/cmenca/new-york-times-hardcover-fiction-best-sellers)
    
    Column              Description
    
     _id:              MongoDB id field
    title:             The book title
    author:            The author(s) of the book
    description:       The NYT provided description
    publisher:         The book publisher
    rank:              The rank on the current list
    rank_last_week:    The rank of the book on last week's list
    weeks_on_list:     The number of weeks a particular book has appeared on the list
    price:             The price given by the NYT
    published_date:    The date the list was published to the NYT
    bestsellers_date:  Week the ranking is based on
    amazon_product_url:The URL to the Amazon product page for this book


##### Analyze Recipes dataset and write code for below operation:

    Explode - The explode method allows you to split an array column into multiple rows, copying all the other columns into each new row.

-  Split the column Ingredients into the column Ingredient, with one ingredient per row.
-  Split the column Method into the column Steps, with one step per row.
  
##### Goal: Do code changes for above operations 

Dataset: src/main/resources/recipes.json

Metadata: (Ref: https://www.kaggle.com/gjbroughton/christmas-recipes)

    The file contains:
    - Recipe Title
    - Recipe Description
    - Recipe Author
    - Ingredients list
    - Step by step method
    

##### How to run spark program through Intellij?
- Set main class as 
    org.apache.spark.deploy.SparkSubmit
- Set program arguments as
   --master local -- class <main_class> target/scala-2.12/<jar_name>
