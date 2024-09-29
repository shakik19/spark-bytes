package com.shakik.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._

import scala.::
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-local")
      .master("local[3]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    val company = "AAPL"
    val applePath = s"dataset/stocks/$company.csv"
    val stockSchema: StructType = StructType(
        StructField("Date", DateType) ::
        StructField("Open", DoubleType) ::
        StructField("High", DoubleType) ::
        StructField("Low", DoubleType) ::
        StructField("Close", DoubleType) ::
        StructField("Adj Close", DoubleType) ::
        StructField("Volume", IntegerType) :: Nil
    )
    val df = spark.read
      .option("header", value = true)
      .schema(stockSchema)
      .csv(applePath)

    df.show(10)
    df.printSchema()

    /**
     *! DOMAIN SPECIFIC LANGUAGE
    */

    /**
     *? COLUMN REFERENCES
    */
    df.select("Date", "Open", "Close").show(10)
    val highCol: Column = df("High")
    val lowCol: Column = col("Low")

    import spark.implicits._

    val nullCount = df.filter(!$"Date".isNull).count()
    assert(nullCount > 0)
    // Conclusion, All methods
    df.select(col("Date"), $"Open", df("Close"), highCol, lowCol).show(20)
    val rowCount: Long = df.count()
    println(rowCount)
    assert(rowCount >= 9909)

    /**
     *? COLUMN FUNCTIONS
     */
    val openPercentage = df("Open").multiply(100).as("Open_Percentage")  // as == alias -> only effects select output
    // the multiply is eqv. to df("Open") * 100 both symbolic and verbal representations are available
    df.select($"Open", openPercentage).show(10)
    val openPercentage2 = concat(round(openPercentage, 2).cast(StringType), lit("%"))
    df.select(openPercentage2.as("Open%")).show(10)

    // returns a new Column. If equal then the row is true and false otherwise
    val equalOpenClose = df("Open") === df("Close") alias "equalOpenClose"
    /*
     === equality operator is specially for columns; isEqual method can be used as well
     =!= means notEqual
     <=> means null safe equal
     CHECK THE COLUMN CLASS, there are more methods like
     contains, like, rike, ilike
     substr, startsWith, endsWith
     withField, getField, dropField
     sorting asc, descNullFirst
     cast etc.
     */

    //                                                           row[i]   cached as it's repeated next line
    println(s"equalOpenClose : ${df.select(equalOpenClose).filter(_(0) == true).cache().count()}")
    df.select(equalOpenClose).filter(_.get(0) == true).show(30)
  }
}