package prScala

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{HashPartitioner,SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.math.pow

object PageRank {
  def main(args: Array[String]): Unit = {
    // Setup and Configuration-----------------------------------------------
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      println("Arguments Error!\nCorrect Usage: <Output directory> <max Links Value> <max Iterations>")
      System.exit(1)
    }
    logger.setLevel(Level.INFO) // Set the logger level

    // Set up Spark configuration
    val conf = new SparkConf().setAppName("PageRank Spark Program")//.setMaster("local[4]")
    val sc = new SparkContext(conf)
    conf.set("spark.driver.memoryOverhead","20g")

    // Generate links------------------Job1----------------------------------
    val maxLinks = args(1).toLong // maxLinks denotes param "k"
    val maxIterations = args(2).toInt // param Iterations
    val TeleportProbability = 0.15 // Random Jump Probability
    var listOfLinks: Seq[(Long, Long)] = Seq.empty

    for (i <- 1 to maxLinks.toInt) {
      val link = if (i != maxLinks) (i.toLong, i.toLong + 1.toLong)
      else (i.toLong, 0.toLong)
      listOfLinks = listOfLinks :+ link
    }

    val numPartitions: Int = if (maxLinks.toInt <= 1000) maxLinks.toInt else (1000)
    // init RDD Processing----------------------------------------------------
    var rdd: RDD[(Long, Long)] = sc.parallelize(listOfLinks,numSlices = numPartitions)

    val hashPartitioner = new HashPartitioner(partitions=numPartitions)
    val links = rdd.partitionBy(hashPartitioner)// Load Balance based on Range Partitioning
      .cache() // For all iterations

    // Initial Rank Values; Updates every iteration!
    // Because no more or less than unique k-chains with unique k-links, where total nodes = maxLinks^2
    var pageRanks: RDD[(Long,Double)] = links.filter(x=>x._1!=0)mapValues(_ => 1.0 / pow(maxLinks, 2)) // Probability (is 1/total nodes)
    var totalRedistributableMass = 0.0

    // Iterative Loop start----------------Iterative Jobs----------------------
    for (iter <- 1 to maxIterations) {
      val contributions = links.join(pageRanks) // Filter dummy node
        .map { // Joining Links with InitPage Ranks
          case (pageID, (outNode, rankVal)) =>
            (outNode,rankVal)
        }.reduceByKey((acc, value) => acc + value)// Use Reduce to combine Dangling mass

      totalRedistributableMass = maxLinks*contributions.lookup(0).head // Check cumulative value of Dangling Mass

      val transformRanks = pageRanks.leftOuterJoin(contributions)
        .map { case (pageID, (outContributions, inContributions)) =>
          (pageID, (inContributions.getOrElse(0.0), totalRedistributableMass/pow(maxLinks, 2)))
        }

      // Update PageRanks Step
      pageRanks = transformRanks.mapValues { case (inContributions, redistributable_mass) =>
        (1 - TeleportProbability) * (inContributions + redistributable_mass)+(TeleportProbability / pow(maxLinks, 2))
      }
//      println(pageRanks.toDebugString)
      pageRanks.persist()
      logger.info(s"\nDangling Mass for Iteration${iter}:${totalRedistributableMass}\n")// Logging Dummy Mass
    }

    val resultRDD: RDD[(Long, Double)] = pageRanks.flatMap { case (node, rank) =>
      (0 until maxLinks.toInt).map { i =>
        (node + i * maxLinks, rank)
      }
    }

    resultRDD.saveAsTextFile(args(0)+s"/Iteration${maxIterations}")
    sc.parallelize(Seq((0.toLong,totalRedistributableMass))).coalesce(1) //add Dummy Node Val for last iteration
      .saveAsTextFile(args(0)+s"/Iteration${maxIterations}"+"/Dummy")// Save separately in main directory
    sc.stop()
  }
}