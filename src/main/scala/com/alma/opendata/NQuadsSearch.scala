package com.alma.opendata

import java.io.StringReader

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

/**
  * Created by thomas on 29/10/16.
  */
object NQuadsSearch {
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
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Search")
    val sc = new SparkContext(conf)

    val dataFile : RDD[String] = sc.textFile(args(0))

    // collect all graphs which are related to a Nantes
    val graphs = sc.broadcast(dataFile.filter(t => t.contains("Nantes") | t.contains("postal-code> \"44"))
      .map(t => getContext(t))
      .distinct()
      .collect().toSet)

    // find and print all triples containing one of the previous graphs
    dataFile.filter(t => {
      graphs.value.contains(getContext(t))
    }).foreach(println)
  }

}
