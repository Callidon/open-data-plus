package com.alma.opendata.spark

import java.io.StringReader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.eclipse.rdf4j.rio.{RDFFormat, RDFParseException, Rio}

object NQuadsLoader {

  def main(args: Array[String]): Unit = {
    val file: String = "src/main/resources/data16.nq"
    val conf: SparkConf = new SparkConf().setAppName("LoadArchive").setMaster("local")
    val sc = new SparkContext(conf)

    val nqFile : RDD[String] = sc.textFile(file)
    nqFile.foreach(t => {
      try {
        val input = new StringReader(t)
        val model = Rio.parse(input, "base:/", RDFFormat.NQUADS)
        println(model)
      } catch {
        case rdfex : RDFParseException => println(rdfex.getMessage)
        case e : Exception => e.printStackTrace()
      }
    })
  }
}
