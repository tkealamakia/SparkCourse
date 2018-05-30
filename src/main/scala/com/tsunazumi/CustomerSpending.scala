package com.tsunazumi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerSpending {

  def parseLine(line : String) = {
    val fields = line.split(",")
    (fields(0), fields(2).toFloat)
  }

  def main(args : Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerSpending")

    val values = sc.textFile("src/main/resources/customer-orders.csv")

    val rdd = values.map(parseLine).reduceByKey( (x,y) => x + y);

    val rddSorted = rdd.map( x => (x._2, x._1) ).sortByKey()

    val results = rddSorted.collect()

    for (result <- results.sorted) {
      val customerId = result._2
      val spent = result._1
      val formatedSpent = f"$spent%.2f"
      println(s"Customer $customerId spent $$$formatedSpent")
    }

  }

}
