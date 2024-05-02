package tc

import org.apache.log4j.{FileAppender, Level, LogManager, PatternLayout}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.functions.broadcast

object triangle_repDF {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Arguments Error!\nCorrect Usage: <input dir> <output dir> <max Value>")
      System.exit(1)
    }

    val layout = new PatternLayout("%d{MM-dd@HH:mm:ss} %-5p (%13F:%L) %3x - %m%n")//logging format
    val fa = new FileAppender(layout,args(3),true)
    logger.setLevel(Level.INFO)//setting info level to get accumulator count at the end
    logger.addAppender(fa)//Configuring logger

    // Set up Spark configuration
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("dataFrame Aggregation Program").getOrCreate()
    val sc = spark.sparkContext
    val accum = sc.longAccumulator("SocialTriangles_RSD")// for Final Triangle count
    val threshold=args(2)

    //Main
    val schema = new StructType()
      .add("midNode",IntegerType,true)//Generated schema for reading as DataFrame
      .add("outNode",IntegerType,false)//Nullable is False(Think as Primary key)
    val data= spark.read.format("CSV").schema(schema).load(args(0))
    val edges=data.filter(data("midNode")<=threshold.toLong).filter(data("outNode")<=threshold.toLong)

    val broadcast_edges = broadcast(edges)
    val path: DataFrame = edges.toDF("firstNode","midNode").join(broadcast_edges,"midNode")
      .where("outNode != firstNode")//Removing Hop Paths
//    path.explain()

    val triangles3: DataFrame = path.withColumnRenamed("outNode","join2Node")
      .join(broadcast_edges.withColumnRenamed("midNode","join2Node"),"join2Node")
      .where(("outNode == firstNode"))
//    triangles3.explain()

    accum.add(triangles3.count())//incrementing accumulator count
    logger.info(s"Social Triangles Count found:${accum.value / 3}")//Logging into root logger
    triangles3.write.format("csv").save(args(1))
  }
}