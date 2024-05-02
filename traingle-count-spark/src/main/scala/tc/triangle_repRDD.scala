package tc

import org.apache.log4j.{FileAppender, Level, LogManager, PatternLayout}
import org.apache.spark.{SparkConf, SparkContext}

object triangle_repRDD {
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

    val outNode_lookup = sc.broadcast(outNode_rdd.groupByKey().collect.toMap)

    val path = outNode_rdd.flatMap {
      case (key, out_value) =>
        outNode_lookup.value.get(out_value).map { values =>
          values.map(x => (key, out_value, x))
        }.getOrElse(Seq.empty[(Long, Long, Long)])
    }.filter(nodes=> nodes._1 != nodes._3)
//    print(path.toDebugString)

    val triangles3 = path.flatMap {
      case (node1,node2,node3) =>
        outNode_lookup.value.get(node3).map { values =>
          values.map(check => (node1, node2, node3,check))
        }.getOrElse(Seq.empty[(Long, Long, Long,Long)])
    }.filter(nodes=> nodes._1 == nodes._4)
//    print(triangles3.toDebugString)

    accum.add(triangles3.count())//incrementing accumulator count
    logger.info(s"Social Triangles Count found:${accum.value/3}")//Logging into root logger
    triangles3.saveAsTextFile(args(1))
  }
}