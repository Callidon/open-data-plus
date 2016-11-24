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

    val nqFile : RDD[String] = sc.textFile(args(0))

    val subjects = nqFile.filter(t => t.contains("Nantes") | t.contains("postal-code> \"44"))
      .map(t => t.split(" ")(0))
      .distinct()
      .collect()

    nqFile.filter(t => subjects.contains(t))
      .foreach(println)
  }

}
