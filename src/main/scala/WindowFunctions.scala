package com.shakik.spark

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val df = GetDataframe.getRenamedColumn(GetDataframe.get("AAPL"))
    val highestClosingPricesPerYear = this.highestClosingPricesPerYear(df)
    highestClosingPricesPerYear.show()
  }

  def highestClosingPricesPerYear(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val window: WindowSpec = Window
      .partitionBy(year($"date").as("year"))
      .orderBy($"close".desc)

    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
  }
}
