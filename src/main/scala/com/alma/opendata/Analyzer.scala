package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeSet

/**
* Web Data Commons Analyzer built with Apache Spark
*/
object Analyzer {
  // vcard + schema predicates
  val VCARD_COUNTRY = "<http://www.w3.org/2006/vcard/ns#country-name>"
  val VCARD_REGION = "<http://www.w3.org/2006/vcard/ns#region>"
  val VCARD_POSTAL_CODE = "<http://www.w3.org/2006/vcard/ns#postal-code>"
  val VCARD_LOCALITY = "<http://www.w3.org/2006/vcard/ns#locality>"

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
  * @return
  */
  def aggregate(files: String, sc : SparkContext): RDD[(String, String)] = {
    val stageTwo : RDD[String] = sc.textFile(files)
    stageTwo.map(nquad => (nquad, nquad.lastIndexOf("<"), nquad.lastIndexOf(">")))
    .flatMap(t => {
      try {
        Some((t._1.substring(t._2 + 1, t._3), t._1.substring(0, t._2 - 1)))
      } catch {
        case e: Exception => None
      }
    })
    .reduceByKey((acc, ntriple) => acc + " . " + ntriple)
  }

  /**
  * Opposite of aggregation: restore the original n-quads format
  * @param data
  * @return
  */
  def desaggregate(data: RDD[(String, String)]) : RDD[String] = data.flatMap(t => t._2.split(" . ").map(_ + "<" + t._1 + "> ."))

  /**
  * Run stage 3 : aggregation + decision tree
  * @param files
  * @param sc
  * @return
  */
  def runStageThree(files: String, sc : SparkContext) : RDD[(String, String)] = {
    aggregate(files, sc)
    .filter(t => t._2.contains("<http://www.w3.org/2006/vcard/ns#country-name> \"France\""))
  }

}
