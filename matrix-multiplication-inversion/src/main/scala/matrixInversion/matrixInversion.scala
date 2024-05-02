package project

import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import breeze.linalg._

object matrixInversionMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("matrix inversion 1")
      // Comment out the next one of next two lines
//      .setMaster("local[4]")
      .setMaster("yarn")
    val sc = new SparkContext(conf)
    conf.set("spark.driver.memoryOverhead", "20g")

    val inputFile = sc.textFile(args(0))
    val mat = inputFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }

    // Create partitioner
    val customPartitioner = new HashPartitioner(5)

    // Convert RDD into breeze matrix
    val inputMatCollect = mat.collect()
    val breezeMatrix = DenseMatrix(inputMatCollect.map(_.toArray):_*)
    val choleskyFactor = cholesky(breezeMatrix)
    val choleskyArray = choleskyFactor.toArray.grouped(choleskyFactor.cols).toArray.transpose
    val colSeq = for (i <- choleskyArray.indices) yield i
    val colRDD = sc.parallelize(colSeq)
    val invRDD = colRDD.map {
      case (colNum) =>
      val invSol = mutable.Map[Int, Double]()
      invSol += 0 -> 0.0
      choleskyArray.zipWithIndex.foreach {
        case (row, index) =>
          var numerator: Double = 0.0
          if (index == 0 && colNum == 0) {
            invSol += 0 -> 1 / row(0)
          } else {
            for (i <- 0 until index) {
              numerator = numerator + row(i) * invSol(i)
            }
            if (index == colNum) numerator = 1 - numerator else numerator = - numerator
            invSol += index -> numerator / row(index)
          }
      }
        // save format, comment out as needed
        (colNum, invSol.toString())
//        (colNum, invSol.toArray)
    }.partitionBy(customPartitioner)
    // line to print data to console
//    invRDD.foreach(row => println("invRDD", row._1, row._2.mkString("Array(", ", ", ")")))

    // Save to file
    invRDD.saveAsTextFile(args(1))
  }


}
