package com.shakik.spark

import TaskSet1._
import WindowFunctions._

import org.apache.spark.sql.functions.year
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date
import scala.language.postfixOps

class Test extends AnyFunSuite {
  private val df = GetDataframe.getRenamedColumn(GetDataframe.get("AAPL"))

  import df.sparkSession.implicits._

  test("Returns highest closing prices/year") {
    val stockSchema: StructType = StructType(
      StructField("date", DateType) ::
        StructField("open", DoubleType) ::
        StructField("close", DoubleType) :: Nil
    )

    implicit val encoder: Encoder[Row] = Encoders.row(stockSchema)
    val testRow = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0, 1),
      Row(Date.valueOf("2022-03-22"), 1.0, 2.0, 1),
      Row(Date.valueOf("2023-01-12"), 8.0, 4.0, 1)
    )
    val expectedRows = Array(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0, 1),
      Row(Date.valueOf("2023-01-12"), 8.0, 4.0, 1)
    )
    val testDf = df.sparkSession.createDataset(testRow)

    val resultRows = highestClosingPricesPerYear(testDf).collect()

    resultRows should contain theSameElementsAs expectedRows
  }

  test("Returns the number of days when 'close' >= 10% higher than open") {
    val result = task3ResultCount(task3(df))
    assert(result >= 22)
  }

  test("Testing WindowFunctions Year Consistency") {
    val totalYears = df.select(year($"date")).distinct().count()
    assert(highestClosingPricesPerYear(df).count() == totalYears)
  }
}
