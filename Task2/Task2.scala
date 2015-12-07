package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
	def main(args: Array[String]) {
    		val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    		// read the file
    		val file = sc.textFile("hdfs://localhost:8020" + args(0))
		//map all the data from the input file
		val edge_mapper = file.map(mapper)

		//collect all the data and sort it for our convinience
		val edge_reducer = edge_mapper.reduceByKey((a, b) => a+b).sortByKey().map(pair => pair._1 + "\t" + pair._2)
	
    		// store output on given HDFS path.
    		edge_reducer.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }

	def mapper(line: String): (Long, Long) = {
	val pairs = line.split("\t+")

	val edge = pairs(1).toLong
	val weight = pairs(2).toLong
	(edge, weight)
	}
}
