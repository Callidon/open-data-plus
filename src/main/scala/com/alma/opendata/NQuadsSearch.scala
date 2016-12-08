package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeSet

/**
  * Web Data Commons Analyze built with Apache Spark
  */
object NQuadsSearch {
  /**
    * Parse a NQuad in String format and get it's context
    * @param nquad The string to parse
    * @return
    */
  def getContext(nquad : String) : String = {
    val begin = nquad.lastIndexOf("<")
    val end = nquad.lastIndexOf(">")
    nquad.substring(begin + 1, end)
  }

  /**
    * Run stage one
    * @param files
    * @param sc
    */
  def runStageOne(files: String, sc : SparkContext) : Unit = {
    val dataFile : RDD[String] = sc.textFile(files)
    dataFile.filter(nquad => nquad.contains("Nantes") | nquad.contains("postal-code> \"44"))
      .map(getContext)
      .distinct()
      .foreach(println)
  }

  /**
    * Run stage two
    * @param files
    * @param stageOneFiles
    * @param sc
    */
  def runStageTwo(files: String, stageOneFiles: String, sc : SparkContext) : Unit = {
    val dataFile : RDD[String] = sc.textFile(files)
    val stageOne  = sc.broadcast(TreeSet[String]() ++ sc.textFile(stageOneFiles).collect().toSet)

    dataFile.filter(nquad => {
      stageOne.value.contains(getContext(nquad))
    }).foreach(println)
  }

  /**
    * Execute the main program
    * @param args
    */
  def main(args: Array[String]) : Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Search")
    val sc = new SparkContext(conf)

    //runStageOne(args(0), sc)
    runStageTwo(args(0), args(1), sc)
  }

}
