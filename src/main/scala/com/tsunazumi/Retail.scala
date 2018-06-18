package com.tsunazumi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Retail {

  def main(args : Array[String])= {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/retail-data/by-day/*.csv")

    import org.apache.spark.sql.functions.{window, column, desc, col}
    staticDataFrame
      .selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)
  }

}
