/* Spark Code to multiply any two matrices read from files.
Implements H-V on A,B on assumption A.rows >> B.rows
For MatMul, A.rows = B.cols
Uses both Broadcast and Shuffle Strategies.
 */
package matmulDense
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.BitSet.empty.ordering
import scala.math.{max, min}
import scala.reflect.ClassTag

object matmulDense {
  def main(args: Array[String]): Unit = {
    // Setup and Configuration-----------------------------------------------
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 6) {
      logger.error("Arguments Error!\nCorrect Usage: <input directory for Matrix A> " +
        "<input directory for Matrix B> <output directory> " +
        "<First Matrix Rows> <Second Matrix Rows> <Num-Workers>")
      System.exit(1)
    }
    logger.setLevel(Level.INFO) // Set the logger level (Required for AWS)

    // Set up Spark configuration //setMaster not required for aws
    val conf = new SparkConf().setAppName("Matrix-Multiplication")//.setMaster("local[4]")
    conf.set("spark.driver.memoryOverhead", "20g")
    val sc = new SparkContext(conf)

    val size = Array(args(3).toInt,args(4).toInt) // Defining rows from args

    // Checking Ratio to determine Algorithm Strategy
    val ratio: Double = size(0) / size(1)
    val num_workers = args(5).toInt

    // Multiplication = A*B--------------------------------------
    // Reading Matrix A and B, Partitions set to min value of size(rows), size/10 or 100
    val data1: RDD[String] = sc.textFile(args(0), minPartitions = myPartitionFunc(size(0),num_workers))
    val data2: RDD[String] = sc.textFile(args(1), minPartitions = myPartitionFunc(size(1),num_workers))

    val numFeatures_A: Int = data1.first().split(",").size
    val numFeatures_B: Int = data2.first().split(",").size

    val HV_Bool: Broadcast[Boolean] = sc.broadcast (if (size(1)<60000 && numFeatures_A>= numFeatures_B) true else false)

    val matA = if (HV_Bool.value) HorizontalPartitioner(data1) else VerticalPartitioner(data1)
    val matB = if (HV_Bool.value) VerticalPartitioner(data2) else HorizontalPartitioner(data1)

    val result = if (numFeatures_A == size(1)) {
      if (ratio >= 1 && HV_Bool.value) MatmulBroadcast(matA, matB, sc)
      else MatmulShuffle(matA, matB, HV_Bool)
    } else {
      logger.error("Arithmetic Error: Matrix inner dimensions do not match! Cannot Multiply.")
      throw new IllegalArgumentException("Matrix inner dimensions do not match! Cannot Multiply.")
    }
    logger.info("NumPartitions taken for Processing: {}")
    logger.info(result.getNumPartitions)
    result.sortBy(_._1)(customComparator, ClassTag[(Int, Int)](classOf[(Int, Int)])).saveAsTextFile(args(2))
  }

  val customComparator: Ordering[(Int, Int)] = new Ordering[(Int, Int)] {
    override def compare(x: (Int, Int), y: (Int, Int)): Int = {
      val result = x._1.compareTo(y._1)
      if (result == 0) {x._2.compareTo(y._2)} else {result}
    }
  }

  def myPartitionFunc(size:Int, num_workers:Int): Int = {
    // Based on Number of Workers
    min(min(min(size,max(2,size/10)), num_workers),100)
  }

  def HorizontalPartitioner(dataA:RDD[String]): (RDD[(Int,Vector[Double])]) = {
    // Horizontal Partitioned Always
    val matA: RDD[(Int,Vector[Double])]= // RDD for distribute,Vector for faster access
      dataA.map(line => line.split(",").map(_.trim.toDouble).toVector)
        .zipWithIndex.map{case (vec, ind)=>(ind.toInt, vec)}

    matA // Return value // Repartition Not Required
  }

  def VerticalPartitioner(dataB: RDD[String]): RDD[(Int, Vector[Double])] = {
    val matB: RDD[Vector[(Double, Int)]] =
      dataB.map(line => line.split(",").map(_.trim.toDouble).zipWithIndex.toVector)

    // Vertically Partition matB
    val VpartitionedData_B: RDD[(Int, Vector[Double])] = matB.flatMap(array =>
        array.map { case (value, index) => (index, value) }
      ).groupBy(_._1) // Group by index
      .mapValues(_.map(_._2).toVector) // Convert to Vector

    VpartitionedData_B // Return value
  }

  def MatmulBroadcast(HPart_data: RDD[(Int, Vector[Double])], VPart_data: RDD[(Int, Vector[Double])], sc: SparkContext): RDD[((Int,Int),Double)] = {
    // Works best with H-V Partitioning, Fast but Data Duplication
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)
    logger.info("Strategy: H-V Partition-Broadcast")
    val lookup = sc.broadcast(VPart_data.collectAsMap())

    val resultRDD:RDD[((Int,Int),Double)] = HPart_data.map{case (ind,vec)=>lookup.value.map(x=>
        ((ind,x._1),vec.zip(x._2)))}
      .flatMap(x=>x.mapValues(ele=>ele.map{nums=>nums._1 * nums._2})).flatMapValues(x=>x).reduceByKey(_+_)

    resultRDD //Return value
  }

  def MatmulShuffle(mat1:RDD[(Int,Vector[Double])], mat2:RDD[(Int,
    Vector[Double])], HV_Bool:Broadcast[Boolean]): RDD[((Int,Int),Double)] = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)
    // Works best with V-H Partitioning, Highly Scalable but Shuffle Time high
    val resultRDD: RDD[((Int,Int),Double)] =
      if (HV_Bool.value) {
      logger.info("Strategy: H-V Shuffle")
      mat1.cartesian(mat2).map { case (indvecA, indvecB) => ((indvecA._1, indvecB._1), indvecA._2.zip(indvecB._2))
        }.flatMap(tuple => tuple._2.map(nums => (tuple._1, nums._1 * nums._2))).reduceByKey(_ + _)
    }else{
      logger.info("Strategy: V-H Shuffle")
      mat1.map(_._2).zip(mat2.map(_._2)).map{ case(vec1, vec2) => vec1.zipWithIndex.map{i => (i,vec2.zipWithIndex)}}
      .flatMap(identity).map{case(ele,vec)=>vec.map(x=>((ele._2,x._2),x._1*ele._1))}
      .flatMap(identity).reduceByKey(_+_)
    }

    resultRDD //Return value
  }
}
