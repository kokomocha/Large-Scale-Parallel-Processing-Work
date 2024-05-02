package prScala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object ReadCheck {

  def main(args: Array[String]): Unit = {
    // Set up Spark configuration
    val conf = new SparkConf().setAppName("ReadCheck").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val maxLinks =args(1).toInt

    // Read files using wholeTextFiles
    val numPartitions: Int = if (maxLinks <= 2000) maxLinks else math.max(maxLinks, 1000)
    val filesRDD : RDD[(String,String)] = sc.wholeTextFiles(args(0),minPartitions = numPartitions)
    val resultRDD:RDD[(Long,Long)] = filesRDD.flatMap { case (filePath, content) =>
        // Extract pairs of digits separated by a comma from each line
        val pattern = """(\d+),(\d+)""".r
        content.split("\\n+").flatMap { line =>
          pattern.findAllIn(line).matchData.map { matchData =>
            (matchData.group(1).toLong, matchData.group(2).toLong)
          }
        }
      }
    sc.stop()
  }
}
