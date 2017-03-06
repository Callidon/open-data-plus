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
			-s1 <data-files>	Run stage one with the given data files
			-s2 <stage1-files> <data-files>		Run stage two using the output of stage 1 and the data files
			-agg <stage2-files> <output-directory>		Run aggregation using the output of stage 2 and store it in a directory
			-tree <aggregate-files>		Run the decision tree using the aggregate data
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
				Analyzer.aggregate(args(1), sc).saveAsTextFile(args(2))
			}
			case "-tree" => {
				if (args.length < 2) {
					println("Invalid number of arguments, see usage: \n" + usage)
					System.exit(0)
				}
				println("coming soon =)")
			}
		}
  }
}
