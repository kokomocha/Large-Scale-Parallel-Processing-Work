package tc

import org.apache.log4j.{FileAppender, Level, LogManager, PatternLayout}
import org.apache.spark.{SparkConf, SparkContext}

object triangle_RSR {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Arguments Error!\nCorrect Usage: <input dir> <output dir> <max Value>")
      System.exit(1)
    }

    val layout = new PatternLayout("%d{MM-dd@HH:mm:ss} %-5p (%13F:%L) %3x - %m%n")
    val fa = new FileAppender(layout,args(3),true)
    logger.setLevel(Level.INFO)
    logger.addAppender(fa)

    // Set up Spark configuration
    val conf = new SparkConf().setAppName("RDD TriangleCount Program").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val accum = sc.longAccumulator("Social Triangles")

    // modified RDD
    val threshold=args(2)
    val outNode_rdd = sc.textFile(args(0))//reading text
      .map(x=>(x.split(",")(0).toLong,x.split(",")(1).toLong)) //Pre-join transform
      .filter(nodes=>(nodes._1<=threshold.toLong))//filtering based on max threshold, Node1
      .filter(nodes=>(nodes._2<=threshold.toLong))//filtering based on max threshold, Node2

    val outNode_reversed =outNode_rdd.map(edge=>(edge._2,edge._1)) //because rdd joins on first value(key)
    val paths = outNode_rdd.join(outNode_reversed)//Inner Join
      .filter(node=>node._2._1 != node._2._2)//check 1st node and second node different to avoid hop-paths
      .map(checkNodes=>(checkNodes._2._2,(checkNodes._2._1,checkNodes._1,checkNodes._2._2)))
//    print(paths.toDebugString)

    val triangles3 = paths.join(outNode_reversed)//Inner Join to check existing path
      .map(checkpath=>checkpath._2)
      .filter(points=>points._1._1 == points._2)//check 1st node and second node same
//    print(triangles3.toDebugString)

    accum.add(triangles3.count())//incrementing accumulator count
    logger.info(s"Social Triangles Count found:${accum.value/3}")//Logging into root logger
    triangles3.saveAsTextFile(args(1))
  }
}