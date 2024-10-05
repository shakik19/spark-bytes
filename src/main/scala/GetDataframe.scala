package com.shakik.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object GetDataframe {
  def get(companyName: String): DataFrame = {
    val spark = SparkSession.builder()
      .appName("spark-local")
      .master("local[3]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    val path = s"dataset/stocks/$companyName.csv"
    val stockSchema: StructType = StructType(
      StructField("Date", DateType) ::
        StructField("Open", DoubleType) ::
        StructField("High", DoubleType) ::
        StructField("Low", DoubleType) ::
        StructField("Close", DoubleType) ::
        StructField("Adj Close", DoubleType) ::
        StructField("Volume", IntegerType) :: Nil
    )
    spark.read
      .option("header", value = true)
      .schema(stockSchema)
      .csv(path)
  }

  def getRenamedColumn(df: DataFrame): DataFrame = {
    df.toDF(
      df.columns
        .map(_
          .toLowerCase
          .replaceAll("\\s+", "_")): _*)
  }
}
