package com.alma.opendata

import java.io.StringReader

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFFormat, RDFParseException, Rio}

/**
  * Created by thomas on 29/10/16.
  */
object GraphStore {

  def strToLong(node : String) : Long = {
    var hash : Long = 2381
    node.foreach(c => hash = ((hash << 5) + hash) + c)
    hash
  }

  // pretty print for triple patterns
  def prettyPrint(t: EdgeTriplet[(String, String), String]): Unit = {
    println(s"<${t.srcAttr._1} ${t.attr} ${t.dstAttr._1}> [${t.srcAttr._2}] .")
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Loader")
      .setSparkHome("SPARK_HOME")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val nqFile : RDD[String] = sc.textFile("src/main/resources/data16.nq")

    // extract tuples from the given NQUADS file
    val tuples  = nqFile.map(t => {
      var vertexes = Array[(Long, (String, String))]()
      var edges = Array[Edge[String]]()
      try {
        val input = new StringReader(t)
        val model = Rio.parse(input, "base:/", RDFFormat.NQUADS).toArray
        val statement = model(0).asInstanceOf[Statement]
        val subj = (strToLong(statement.getSubject.toString), (statement.getSubject.toString, statement.getContext.toString))
        val obj = (strToLong(statement.getObject.toString), (statement.getObject.toString, statement.getContext.toString))
        val edge = Edge(strToLong(statement.getSubject.toString), strToLong(statement.getObject.toString), statement.getPredicate.toString)
        vertexes = Array(subj, obj)
        edges = Array(edge)
      } catch {
        case rdfex : RDFParseException => println(rdfex.getMessage)
        case e : Exception => e.printStackTrace()
      }
      (vertexes, edges)
    }).fold((Array[(Long, (String, String))](), Array[Edge[String]]()))((acc, tuple) => (acc._1 ++ tuple._1, acc._2 ++ tuple._2))

    // build the corresponding RDF graph
    val vertexes : RDD[(Long, (String, String))] = sc.parallelize(tuples._1)
    val edges : RDD[Edge[String]] = sc.parallelize(tuples._2)
    val graph = Graph(vertexes, edges)

    // show all tuples
    graph.triplets.foreach(t => prettyPrint(t))
  }

}
