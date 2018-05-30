package com.tsunazumi

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularSuperHero {


  def parseNames (line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    }
    else {
      return None
    }

  }
  def loadHeroNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var heroNames:Map[Int, String] = Map()
    heroNames += (0 -> "Unknown")

    val lines = Source.fromFile("src/main/resources/Marvel-names.txt").getLines()
    for (line <- lines) {
      var fields = line.split('\"')
      if (fields.length > 1) {
        heroNames += (fields(0).trim().toInt -> fields(1))
      }
    }

    return heroNames
  }

  def countCoOccurences(line: String) = {
    val fields = line.split("\\s+")
    (fields(0).toInt, fields.length - 1)
  }

  def main (args: Array[String]) = {


    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperHero")

    var heroNameDict = sc.broadcast(loadHeroNames)

    val relations = sc.textFile("src/main/resources/Marvel-graph.txt")

    val relationsMap = relations.map(countCoOccurences)

    val aggregatePopularity = relationsMap.reduceByKey( (x,y) => x + y)

    val flipped = aggregatePopularity.map( x => (x._2, x._1)).sortByKey()

    val flippedBack = flipped.map( x => (x._2, x._1))

    for (result <- flippedBack) {
      val count = result._2
      val hero = heroNameDict.value(result._1)
      println(s"$hero: $count")
    }

  }

}
