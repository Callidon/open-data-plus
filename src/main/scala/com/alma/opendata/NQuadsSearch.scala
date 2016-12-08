package com.alma.opendata

import java.io.StringReader

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.collection.immutable.{TreeMap, TreeSet}

/**
  * Web Data Commons Analyze built with Apache Spark
  */
object NQuadsSearch {
  /**
    * Parse a NQuad in String format and get it's context
    * @param line The string to parse
    * @return
    */
  def getContext(line : String) : String = {
    try {
      val reader = new StringReader(line)
      val model = Rio.parse(reader, "base:", RDFFormat.NQUADS).toArray
      val statement = model(0).asInstanceOf[Statement]
      statement.getContext.toString
    } catch {
      case e: Exception => ""
    }
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
      val begin = nquad.lastIndexOf("<")
      val end = nquad.lastIndexOf(">")
      stageOne.value.contains(nquad.substring(begin + 1, end))
    }).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Search")
        .setMaster("local")
    val sc = new SparkContext(conf)

    //runStageOne(args(0), sc)
    runStageTwo(args(0), args(1), sc)
  }

}
