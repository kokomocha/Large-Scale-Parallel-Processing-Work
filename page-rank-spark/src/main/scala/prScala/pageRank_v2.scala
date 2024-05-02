package prScala
import org.apache.log4j.{Level, LogManager, Logger, PatternLayout, FileAppender}
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import scala.collection.mutable
import scala.math.pow

object PageRank_v2 {
  def main(args: Array[String]): Unit = {
    // Setup and Configuration-----------------------------------------------
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Arguments Error!\nCorrect Usage: <output directory> " +
        "<max Links Value> <max Iterations>")
      System.exit(1)
    }
    //log4j.properties (Not required for AWS)
    //    val layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p (%13F:%L) %3x - %m%n")
    //    val fileAppender = new FileAppender(layout, args(3), true)
    //    logger.addAppender(fileAppender) // Add the FileAppender to the logger
    logger.setLevel(Level.INFO) // Set the logger level (Required for AWS)

    // Set up Spark configuration //setMaster not required for aws
    val conf = new SparkConf().setAppName("PageRank Spark Program")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Generate links------------------Job1----------------------------------
    val maxLinks = args(1).toLong // maxLinks denotes param "k"
    val maxIterations = args(2).toInt // param Iterations
    val TeleportProbability = 0.15
    val listOfLinks: mutable.MutableList[(Long, Long)] = mutable.MutableList()

    // For loop to create unique k-chains of unique k-links.
    //Updated code for AWS cluster
    for (i <- 1 to maxLinks.toInt) {
      val link = if (i != maxLinks) (i.toLong, i.toLong + 1)
      else (i.toLong, 0.toLong)
      listOfLinks += link
    }

    // init RDD Processing----------------------------------------------------
    val numPartitions: Long = if (maxLinks <= 1000) maxLinks else (1000)
    val RDD = sc.parallelize(listOfLinks.toSeq, numSlices = numPartitions.toInt) // Convert Mutable List to RDD

    // Use range partitioner with groupByKey based on "k" explicit Partitions
    val rangePartitioner = new RangePartitioner(partitions = numPartitions.toInt, rdd = RDD)
    val links = RDD.groupByKey(rangePartitioner).map(x => (x._1.toLong, x._2)) // Load Balance based on Range Partitioning
    val links_lookup = sc.broadcast(links.collectAsMap()) // Broadcast for faster join

    // Initial Rank Values; Updates every iteration!
    // Because no more or less than unique k-chains with unique k-links, where total nodes = maxLinks^2
    var pageRanks: RDD[(Long, Double)] = links.map(x => (x._1.toLong, 1.0 / pow(maxLinks, 2))) // Probability (is 1/total nodes)
      .partitionBy(rangePartitioner)// Partition by Partitioner and Cache
    var redistributable_mass = 0.0

    // Iterative Loop start----------------Iterative Jobs----------------------
    for (iter <- 1 to maxIterations) {
      // Use explicitly coded Broadcast join
      val contributions: RDD[(Long, Double)] = pageRanks.flatMap { case (pageID, rankVal) =>
        links_lookup.value.get(pageID) match {
          case Some(nodeSeq) if nodeSeq.nonEmpty =>
            nodeSeq.map { dest => (dest, rankVal / nodeSeq.size) }
        }
      }.map(x => (x._1.toLong, x._2)).reduceByKey(_ + _)

      redistributable_mass = maxLinks * contributions.lookup(0).headOption.getOrElse(0.0) // Check cumulative value of Dangling Mass

      //Regular Join
      val transformRanks = pageRanks.leftOuterJoin(contributions) // Left Outer to exclude Dummy Page 0
        .map { case (pageID, (outContributions, inContributions)) => (pageID,
          (inContributions.getOrElse(0.0), redistributable_mass / pow(maxLinks, 2))) // Probability (is 1/total nodes)
        }

      // Update PageRanks Step
      pageRanks = transformRanks.mapValues { case (inContributions, redistributable_mass) =>
        (1 - TeleportProbability) * (inContributions + redistributable_mass) + (TeleportProbability / pow(maxLinks, 2))
      }
      logger.info(s"\nDangling Mass for Iteration${iter}:${redistributable_mass}\n") // Logging Dummy Mass
    }
    // Code for filling up missing chains
    val pageRanks_copy: RDD[(Long, Double)] = pageRanks
    val partitioner_final = new RangePartitioner(partitions=numPartitions.toInt, rdd=pageRanks_copy)
    for (j <- 1 until maxLinks.toInt) {
      pageRanks = pageRanks.union(pageRanks_copy
        .partitionBy(partitioner_final)
        .map { case (pageID, rank) =>
        ((pageID + j * maxLinks, rank))
      })
    }
    pageRanks.coalesce(numPartitions.toInt).saveAsTextFile(args(0)+s"/Iteration${maxIterations}")
    sc.parallelize(Seq((0.toLong, redistributable_mass))).coalesce(1) //add Dummy Node Val for last iteration
      .saveAsTextFile(args(0)+s"/Iteration${maxIterations}"+"/Dummy")// Save separately in main directory
  }
}