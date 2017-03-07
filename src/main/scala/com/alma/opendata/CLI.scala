package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.alma.opendata.Analyzer

/**
* Command Line Interface for Open Data Plus analyzer
*/
object CLI {
  case class CLIError(message: String) extends Exception(message)

  val usage = """
  Open Data Plus Crawler: Analyze Web Data Commons using Apache Spark
  Usage: <operation> <options>
  Operations:
    -s1 <data-files>    Run stage 1 with the given data files
    -s2 <stage1-files> <data-files>    Run stage 2 using the output of stage 1 and the data files
    -agg <stage2-files> <output-directory>    Run aggregation using the output of stage 2 and store it in a directory
    -s3 <stage2-files> <output-directory>    Run stage 3 (aggregation + decision tree) using the output of stage 2 and store it in a directory
  """

  def main(args: Array[String]) : Unit = {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }
    val conf: SparkConf = new SparkConf()
    .setAppName("Open Data Plus Crawler")
    val sc = new SparkContext(conf)

    args(0) match {
      case "-s1" => {
        if (args.length < 2) {
          println("Invalid number of arguments, see usage: \n" + usage)
          System.exit(0)
        }
        Analyzer.runStageOne(args(1), sc)
      }
      case "-s2" => {
        if (args.length < 3) {
          println("Invalid number of arguments, see usage: \n" + usage)
          System.exit(0)
        }
        Analyzer.runStageTwo(args(1), args(2), sc)
      }
      case "-agg" => {
        if (args.length < 3) {
          println("Invalid number of arguments, see usage: \n" + usage)
          System.exit(0)
        }
        Analyzer.aggregate(args(1), sc).saveAsSequenceFile(args(2))
      }
      case "-s3" => {
        if (args.length < 3) {
          println("Invalid number of arguments, see usage: \n" + usage)
          System.exit(0)
        }
        val results = Analyzer.runStageThree(args(1), sc)
        Analyzer.desaggregate(results).saveAsTextFile(args(2))
      }
      case _ => println("Invalid arguments, see usage: \n" + usage)
    }
  }
}
