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

    // collect all subjects which are related to a Nantes
    val subjects = dataFile.filter(t => t.contains("Nantes") | t.contains("postal-code> \"44"))
      .map(t => t.split(" ")(0))
      .distinct()
      .collect()

    // find and print all triples containing one of the previous subjects
    dataFile.filter(t => subjects.contains(t.split(" ")(0)))
      .foreach(println)
  }

}
