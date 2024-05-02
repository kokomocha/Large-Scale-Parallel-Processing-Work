package linearRegression

import matmulGeneric.matMul_ver0.{hv_matmul, vh_matmul}
import breeze.linalg.{DenseMatrix, cholesky}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, rdd}

import scala.collection.mutable

object linearRegressionFinalMain {
  def matMulFile(conf: SparkConf, sparkContext: SparkContext, inputFilePath: String, partitionSize: Int): RDD[((Int, Int), Double)] = {
    val inputFile = sparkContext.textFile(inputFilePath)
    val customPartitioner = new HashPartitioner(partitionSize)
    val mat = inputFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }
    val numFeatures = mat.first().length

    val matMul = mat.flatMap {
      case (line) =>
        line.zipWithIndex.flatMap {
          case (element, index) =>
            (0 until numFeatures - index).map { i =>
              ((index, numFeatures - i - 1), line(numFeatures - i - 1) * element)
            }
        }
    }.partitionBy(customPartitioner).reduceByKey((x, y) => x + y)
    matMul
  }

  // Method for ATA matrix multiplication with matrix as input
  def ATAMatMulRDD(conf: SparkConf, sparkContext: SparkContext, mat: RDD[Array[Double]], partitionSize: Int): RDD[((Int, Int), Double)] = {
    val customPartitioner = new HashPartitioner(partitionSize)
    val numFeatures = mat.first().length

    val matMul = mat.flatMap {
      case (line) =>
        line.zipWithIndex.flatMap {
          case (element, index) =>
            (0 until numFeatures - index).map { i =>
              ((index, numFeatures - i - 1), line(numFeatures - i - 1) * element)
            }
        }
    }.partitionBy(customPartitioner).reduceByKey((x, y) => x + y)
    matMul
  }

  // Method for matrix inversion
  def MatrixInversion(conf: SparkConf, sc: SparkContext, inputMat: RDD[Array[Double]], partitionNumber: Int): RDD[Array[Double]] = {
    // Create partitioner
    val customPartitioner = new HashPartitioner(partitionNumber)

    // Convert RDD into breeze matrix
    val inputMatCollect = inputMat.collect()
    val breezeMatrix = DenseMatrix(inputMatCollect.map(_.toArray):_*)
    val choleskyFactor = cholesky(breezeMatrix)
    val choleskyArray = choleskyFactor.toArray.grouped(choleskyFactor.cols).toArray.transpose
    val colSeq = for (i <- choleskyArray.indices) yield i
    val colRDD = sc.parallelize(colSeq)
    val invPairRDD = colRDD.map {
      case (rowNum) =>
        val invSol = mutable.Map[Int, Double]()
        invSol += 0 -> 0.0
        choleskyArray.zipWithIndex.foreach {
          case (row, index) =>
            var numerator: Double = 0.0
            if (index == 0 && rowNum == 0) {
              invSol += 0 -> 1 / row(0)
            } else {
              for (i <- 0 until index) {
                numerator = numerator + row(i) * invSol(i)
              }
              if (index == rowNum) numerator = 1 - numerator else numerator = - numerator
              invSol += index -> numerator / row(index)
            }
        }
        // save format, comment out as needed
        //        (invSol.toString())
        (rowNum, invSol.toList.sortBy(_._1).map(_._2).toArray)
    }.partitionBy(customPartitioner)

    val invRDD = invPairRDD.map{case (k,v) => v}
    transposeArrays(invRDD)
  }

  // Class for matrix transformations
  //class MatrixTransforms {
  // Convert triangle to full matrix
  def triangleToFullMat(lowerTriangle: RDD[((Int, Int), Double)]): RDD[Array[Double]] = {//RDD[((Int, Int), Double)] = {
    val upperTriangle = lowerTriangle.filter { case ((row, col), _) =>
        // Exclude diagonal elements
        row != col }
      .map {
        case ((row, col), value) => ((col, row), value)
      }

    val combinedRDD = lowerTriangle.union(upperTriangle)

    val rowsRDD = combinedRDD
      .map { case ((row, col), value) => (row, (col, value)) }
      .groupByKey()
      .mapValues(_.toArray.sortBy(_._1).map(_._2)) // Using mapValues here
      .sortByKey()
      .values

    rowsRDD
  }

  // Method to transpose RDD
  def transposeArrays(mat :RDD[Array[Double]]): RDD[Array[Double]] = {
    // Zip each array element with its index
    val zippedRDD = mat.map(_.zipWithIndex)

    // Perform the transpose by swapping keys and values
    val transposedRDD = zippedRDD.flatMap { array =>
      array.map { case (value, index) => (index, value) }
    }.groupByKey()

    // Sort by key and convert back to Array[Double]
    transposedRDD.sortByKey().map { case (_, values) => values.toArray }
  }

  def pairRDDToRows(pairRDD: RDD[((Int, Int), Double)], numCols: Int): RDD[Array[Double]] = {
    // Group values by the row index and sort the groups by row index
    val groupedRDD = pairRDD.groupBy { case ((row, _), _) => row }.sortByKey()

    // Sort grouped values by column index and collect them into arrays
    val rowsRDD = groupedRDD.mapValues { values =>
      // Initialize an array to hold the row values
      val rowArray = Array.fill[Double](numCols)(0.0)

      if (numCols == 1) {
        // If there's only one column, no need to sort, just fill in the array directly
        values.foreach { case ((_, _), value) =>
          rowArray(0) = value
        }
      } else {
        // Fill in the row array with values from the grouped pairs, sorted by column index
        values.toSeq.sortBy { case ((_, col), _) => col }.foreach { case ((_, col), value) =>
          rowArray(col) = value
        }
      }

      rowArray
    }

    // Extract the arrays representing rows
    rowsRDD.map { case (_, rowArray) => rowArray }
  }

  // Perform linear regression on inputs
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("linReg")
//      .setMaster("local[4]")
    .setMaster("yarn")
    val sc = new SparkContext(conf)

    // Input files - for local run
//    val inputFiles = args(0).split(",")
//    val trainAFilePath = inputFiles(0)
//    val testAFilePath = inputFiles(1)
//    val trainbFilePath = inputFiles(2)
//    val testbFilePath = inputFiles(3)

    // Input files for aws
    val trainAFilePath = args(0)
    val testAFilePath = args(1)
    val trainbFilePath = args(2)
    val testbFilePath = args(3)

    val partitionNumber = 3

    val trainAFile = sc.textFile(trainAFilePath, partitionNumber)
    val trainbFile = sc.textFile(trainbFilePath, partitionNumber)
    val testAFile = sc.textFile(testAFilePath, partitionNumber)
    val testbFile = sc.textFile(testbFilePath, partitionNumber)

    val trainA: RDD[Array[Double]] = trainAFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }
    val trainb: RDD[Array[Double]] = trainbFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }
    val testA: RDD[Array[Double]] = testAFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }
    val testb: RDD[Array[Double]] = testbFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toDouble))
    }

    // Partitioner variables
    val inversionPartitionerNumber = 3

    // Statistics of training data
    val numFeatures = trainA.first().length
    val numRowsTrain = trainA.count().toInt // Scala limits

    // Calculate ATA matrix (note multiplication only returns lower triangle since it is symmetrical)
    val ATALowerT = ATAMatMulRDD(conf, sc, trainA, 1)
    val ATAFull = triangleToFullMat(ATALowerT)  // Convert to full matrix
    println("ATAFull First: ", ATAFull.first().slice(0, 10).mkString("Array(", ", ", ")"))
    println("ATAFull Size: ", ATAFull.count().toString, ATAFull.first().length.toString)

    val ATAFullBreeze = DenseMatrix(ATAFull.collect().map(_.toArray):_*)

    println("Breeze Mat:", ATAFullBreeze)
//    println()
    val breezeInv = breeze.linalg.inv(ATAFullBreeze)
    val breezeArray = breezeInv.toArray.grouped(breezeInv.cols).toArray
    val breezeInvRDD = sc.parallelize(breezeArray)

    println("breezeInv", breezeInvRDD.first().slice(0, 10).mkString("Array(", ", ", ")"))

    // Invert matrix - ATA^-1 (also returns lower triangle since uses ATA matrix multiplcation)
//    val ATAInvLowerT = MatrixInversion(conf, sc, ATAFull, inversionPartitionerNumber)
//    val ATAInvFull = triangleToFullMat(ATAMatMulRDD(conf, sc, ATAInvLowerT, 1))
//    println("ATAInvFull First: ", ATAInvFull.first().slice(0, 10).mkString("Array(", ", ", ")"))
//    println("ATAInvFull Size: ", ATAInvFull.count().toString, ATAInvFull.first().length.toString)

    val AT = transposeArrays(trainA)

    // ATAInv * AT
//    val ATAInvAT = hv_matmul(ATAInvFull, AT, sc)
    val ATAInvAT = hv_matmul(breezeInvRDD, AT, sc)
    val ATAInvATMatrix = pairRDDToRows(ATAInvAT, numRowsTrain)

    // For printing purposes
//    println(ATAInvATMatrix.toString())
//    ATAInvATMatrix.zipWithIndex.foreach { case (row, rowNum) =>
//      println(s"ATAInv Row $rowNum: " + row.mkString(", "))
//    }

    println("ATAInvAT First: ", ATAInvATMatrix.first().slice(0, 10).mkString("Array(", ", ", ")"))
    println("ATAInvAT Size: ", ATAInvATMatrix.count().toString, ATAInvATMatrix.first().length.toString)

    // B = ATAInv * AT * b
    val B = hv_matmul(ATAInvATMatrix, trainb, sc)
    val BMatrix = pairRDDToRows(B, 1)

//    BMatrix.zipWithIndex.foreach { case (row, rowNum) =>
//      println(s"BMatrix Row $rowNum: " + row.mkString(", "))
//    }

    println("BMat First: ", BMatrix.first().slice(0, 10).mkString("Array(", ", ", ")"))


    // Get prediction - y_pred = B * testA
    val pred = hv_matmul(testA, BMatrix, sc)

    val predVector = pairRDDToRows(pred, 1).coalesce(1).collect()

//    predVector.zipWithIndex.foreach { case (row, rowNum) =>
//      println(s"pred Row $rowNum: " + row.mkString(", "))
//    }

    val repartitionTestB = testb.coalesce(1).collect()

    // Calculate error
    val squaredError: Double = repartitionTestB.zipWithIndex.map { case (testArray, index) =>
      val predArray = predVector(index) // Get the corresponding prediction array
      testArray.zip(predArray).map { case (testValue, predValue) =>
        val error = testValue - predValue
        error * error
      }.sum
    }.sum
    val numSamples = predVector.length.toDouble
    val mse = squaredError / numSamples
//    val predStr = predVector.map(_.mkString("Array(", ", ", ")")).mkString(", ")
    print("num samples", numSamples)
    print("mse: ", mse)
  }
}
