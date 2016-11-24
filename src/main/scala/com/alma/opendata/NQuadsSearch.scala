package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by thomas on 29/10/16.
  */
object NQuadsSearch {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Search")
    val sc = new SparkContext(conf)

    val dataFile : RDD[String] = sc.textFile(args(0))

    // collect all graphs which are related to a Nantes
    val graphs = sc.broadcast(dataFile.filter(t => t.contains("Nantes") | t.contains("postal-code> \"44"))
      .map(t => t.split(" ")(3))
      .distinct()
      .collect().toSet)

    // find and print all triples containing one of the previous graphs
    dataFile.filter(t => graphs.value.contains(t.split(" ")(3)))
      .foreach(println)
  }

}
