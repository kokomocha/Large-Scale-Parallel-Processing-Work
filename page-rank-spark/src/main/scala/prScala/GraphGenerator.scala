package prScala
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object GraphGenerator {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    logger.setLevel(Level.DEBUG)
    if (args.length != 2) {
      logger.error("Arguments Error!\nCorrect Usage: <output directory> " +
        "<max Links Value>")
      System.exit(1)
    }
    // Set up Spark configuration
    val conf = new SparkConf().setAppName("GraphGenerator")//.setMaster("local[4]")
    val sc = new SparkContext(conf)
    conf.set("spark.driver.memoryOverhead","20g")
    val maxLinks = args(1).toLong

    try {
      // Use Partitions to create list of links
      val numPartitions: Int = if (maxLinks < 100) maxLinks.toInt else 100
      // For loop to create unique k-chains of unique k-links.
      var listOfLinks: Seq[(Long, Long)] = Seq.empty

      for (i <- 1 until maxLinks.toInt) {
        val link = (i.toLong, i.toLong + 1.toLong)
        listOfLinks = listOfLinks :+ link
      }

      // init RDD Processing----------------------------------------------------
      val hashPartitioner = new HashPartitioner(partitions=numPartitions)
      val rdd: RDD[(Long, Long)] = sc.parallelize(listOfLinks,numPartitions).partitionBy(hashPartitioner)

      val resultRDD: RDD[(Long, Long)] = rdd.flatMap { case (node1, node2) =>
        (0 until maxLinks.toInt).map { i =>
            (node1 + i * maxLinks, node2 + i * maxLinks)
        }
      }
      resultRDD.map(x=>s"${x._1},${x._2}").saveAsTextFile(args(0))
    } finally {
      sc.stop()
    }
  }
}
