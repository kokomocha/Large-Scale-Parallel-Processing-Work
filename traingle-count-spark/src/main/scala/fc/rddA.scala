package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object rddA {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Arguments Error!\nCorrect Usage: <input dir> <output dir>")
      System.exit(1)
    }

    //Configuration Changes
    val conf = new SparkConf().setAppName("RDD AggregateByKey Program").setMaster("local[4]")
    //Context File sc
    val sc = new SparkContext(conf)

    //Main
    val rdd = sc.textFile(args(0))//reading text
    val count = rdd.map(line => line.split(","))
      .map { case Array(p1, p2) => (p2.toInt, 1) }
      .filter { case (x, _) => x % 100 == 0 }
      .aggregateByKey(0)(
        (count, followers) => count + followers,
        (count1, count2) => count1 + count2
      )
//    print(count.toDebugString)
    count.saveAsTextFile(args(1))
  }
}