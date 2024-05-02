package matmulATA

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

class ATAMatmulClass {
  def matMulFile(conf: SparkConf, sparkContext: SparkContext, inputFilePath: String, partitionSize: Int): RDD[((Int, Int), Double)] = {
    val inputFile = sparkContext.textFile(inputFilePath)
    val customPartitioner = new HashPartitioner(partitionSize)
    val mat = inputFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }
    val numFeatures = mat.first().length

  val matMul = mat.flatMap { line =>
    val indexedLine = line.zipWithIndex
    indexedLine.flatMap { case (element, index) =>
      val otherIndexedElements = line.drop(index).zipWithIndex
      otherIndexedElements.map { case (otherElement, j) =>
        ((index, index + j), otherElement * element)
      }
    }
  }.reduceByKey(_ + _)
    matMul
  }

  def matMulRDD(conf: SparkConf, sparkContext: SparkContext, mat: RDD[Array[Double]], partitionSize: Int): RDD[((Int, Int), Double)] = {
    val customPartitioner = new HashPartitioner(partitionSize)
    val numFeatures = mat.first().length

    val matMul = mat.flatMap { line =>
      val indexedLine = line.zipWithIndex
      indexedLine.flatMap { case (element, index) =>
        val otherIndexedElements = line.drop(index).zipWithIndex
        otherIndexedElements.map { case (otherElement, j) =>
          ((index, index + j), otherElement * element)
        }
      }
    }.reduceByKey(_ + _)
    matMul
  }
}

object ATAMatMulMain {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ATAMatMul")
      .setMaster("local[4]")
//      .setMaster("yarn")
    val sc = new SparkContext(conf)
    val ATAMatMulObject = new ATAMatmulClass()
    val matMulLowerT = ATAMatMulObject.matMulFile(conf, sc, args(0), 8)
//    matMulLowerT.foreach{case ((row,col), value) => println("row: " + row + " col: " + col + " val: " + value)}
    matMulLowerT.saveAsTextFile(args(1))

  }
}
