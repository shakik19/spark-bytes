package com.shakik.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, max}

object TaskSet1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark-bytes")
      .master("local[3]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    import spark.implicits._

    val df = GetDataframe.get("AAPL")

    /**
     * ? Task 1: Rename all the column name to 'camel_case'
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
     * ? Task 2: Add a column containing the diff between 'open' and 'close'
     */
    df2.withColumn("diffOpenClose", df2("open") - df2("close"))
      .select($"open", $"close", $"diffOpenClose")
      .show(5)

    /**
     * ? Task 3: Find the days when 'close' price was more that 10% higher than open
     */
    df2.select($"date", $"open", $"close")
      .where($"close" > $"open" * 1.10)
      .show()

    /**
     * ? Task 4: Find the maximum volume of share for every month in ascending order.
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
