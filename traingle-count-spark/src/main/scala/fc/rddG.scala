package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object rddG {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Arguments Error!\nCorrect Usage: <input dir> <output dir>")
      System.exit(1)
    }

    //Configuration Changes
    val conf = new SparkConf().setAppName("RDD GroupByKey Program").setMaster("local[4]")
    //Context File sc
    val sc = new SparkContext(conf)

    //Main
    val pair_rdd = sc.textFile(args(0))//reading text
    val count = pair_rdd.map(line => line.split(","))
      .map { case Array(p1, p2) => (p2.toInt, 1) }
      .filter { case (x, _) => x % 100 == 0 }//ids divisible by 100
      .groupByKey()//GroupByKey
      .map(n => (n._1, n._2.sum))//separate map for aggregation
//    print(count.toDebugString)
    count.saveAsTextFile(args(1))
  }
}