package com.shakik.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._

import scala.::
object ColumnFunctionality {
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
    println(s"Row Count: $rowCount")
    assert(rowCount >= 9909)

    /**
     *? COLUMN FUNCTIONS
     */
    val openPercentage = df("Open").multiply(100).as("Open_Percentage")  // as == alias -> only effects select output
    // the multiply is eqv. to df("Open") * 100 both symbolic and verbal representations are available
    df.select($"Open", openPercentage).show(10)
    val openPercentage2 = concat(round(openPercentage, 2).cast(StringType), lit("%"))
    df.select(openPercentage2.as("Open%")).show(10)


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

    // returns a new Column. If equal then the row is true and false otherwise
    val equalOpenClose = df("Open") === df("Close") alias "equalOpenClose"

    //                                                           row[i]   cached as it's repeated next line
    println(s"equalOpenClose : ${df.select(equalOpenClose).filter(_(0) == true).cache().count()}")
    df.select(col("Open"), col("Close"), equalOpenClose).filter(_.get(2) == true).show(5)

    // Adding a new Column with given column or given expression (matches for every row)
    df.withColumn("isOpenEqualClose", $"Open" === $"Close").select($"Open", $"Close", $"isOpenEqualClose").show(5)



    /**
     *? Task 1: Rename all the column name to 'camel_case'
     */
    val renamedColumn = Seq(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adj_close"),
      col("Volume").as("volume")
    )
    df.select(renamedColumn: _*).printSchema()
    // This approach is the most explicit and simple but requires every name to be strongly specified

    // OR
    // here df is passed to the lambda func as first arg tempDf and col is the left most element of df.columns seq
    df.columns.foldLeft(df)((tempDf, col) =>
      tempDf.withColumnRenamed(col, col.toLowerCase.replaceAll("\\s+", "_"))).printSchema()
    // This method provides more versatility to control the logic for each column since in each iteration a new df is created

    // OR
    // This method is more simpler but lacks individual control. Good for uniform renaming use cases
    df.select(df.columns.map(c => col(c).as(c.toLowerCase.replaceAll("\\s+", "_"))): _*).printSchema()
    val df2 = df.toDF(df.columns.map(_.toLowerCase.replaceAll("\\s+", "_")): _*)
    df2.printSchema()

    // the _* indicates to unpack all the elements of the Collection

    // OR using SQL expr.

    /**
     *? Task 2: Add a column containing the diff between 'open' and 'close'
     */
    df2.withColumn("diffOpenClose", df2("open") - df2("close"))
      .select($"open", $"close", $"diffOpenClose")
      .show(5)

    /**
     *? Task 3: Find the days when 'close' price was more that 10% higher than open
     */
    df2.select($"date", $"open", $"close")
      .where($"close" > $"open" * 1.10)
      .show()

    /**
     *? Task 4: Find the maximum volume of share for every month in ascending order.
     */
    df2.createOrReplaceTempView("df")
    spark.sql(
      """
        |SELECT
        | DATE_FORMAT(df.date, "MM-yy") AS month,
        | MAX(volume) AS max_volume
        |FROM
        | df
        |GROUP BY
        | 1
        |ORDER BY
        | 1
        |""".stripMargin).show()
    // Equivalent Code
    df2.withColumn("month", date_format(col("date"), "MMM-yyyy"))
      .groupBy("month")
      .agg(max("volume").as("max_volume"))
      .orderBy($"max_volume".desc_nulls_first)
      .show()
  }
}