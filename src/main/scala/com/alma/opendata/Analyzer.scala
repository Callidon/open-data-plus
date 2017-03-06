package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeSet

/**
  * Web Data Commons Analyzer built with Apache Spark
  */
object Analyzer {
  /**
    * Parse a NQuad in String format and get it's context
    * @param nquad The string to parse
    * @return The context of the nquad
    */
  def getContext(nquad : String) : String = {
    val begin = nquad.lastIndexOf("<")
    val end = nquad.lastIndexOf(">")
    nquad.substring(begin + 1, end)
  }

	/**
	 * Get a n-quad as a n-triples, i.e. without the 4th element
	 * @param nquad The string to parse
	 * @return The n-quad as a n-triple
	 */
	def asNTriple(nquad: String) : String = {
		val begin = nquad.lastIndexOf("<")
		nquad.substring(0, begin - 1)
	}

  /**
    * Run stage one
    * @param files
    * @param sc
    */
  def runStageOne(files: String, sc : SparkContext) : Unit = {
    val dataFile = sc.textFile(files)
    dataFile.filter(nquad => nquad.contains("Nantes") | nquad.contains("postalCode> \"44") | nquad.contains("postal-code> \"44"))
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
	 * Aggregate results and index them by graph
	 * @param files
	 * @param sc
	 */
	def aggregate(files: String, sc : SparkContext): RDD[(String, String)] = {
		val stageTwo : RDD[String] = sc.textFile(files)
		stageTwo.map(nquad => (nquad, nquad.lastIndexOf("<"), nquad.lastIndexOf(">")))
		.filter(t => t._2 > 0 && t._2 + 1 <= t._1.length && t._3 <= t._1.length)
		.map(t => (t._1.substring(t._2 + 1, t._3), t._1.substring(0, t._2 - 1)))
		.reduceByKey((acc, ntriple) => acc + " . " + ntriple)
	}
}
