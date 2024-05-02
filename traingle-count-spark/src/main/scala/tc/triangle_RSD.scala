package tc

import org.apache.log4j.{FileAppender, Level, LogManager, PatternLayout}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}

object triangle_RSD {
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
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val accum = sc.longAccumulator("SocialTriangles_RSD")// for Final Triangle count
    val threshold=args(2)

    //Main
    val schema = new StructType()
      .add("midNode",IntegerType,true)//Generated schema for reading as DataFrame
      .add("outNode",IntegerType,false)//Nullable is False(Think as Primary key)
    val data= spark.read.format("CSV").schema(schema).load(args(0))
    val edges=data.filter(data("midNode")<=threshold.toLong).filter(data("outNode")<=threshold.toLong)

    val path: DataFrame = edges.alias("df1").withColumnRenamed("midNode","inNode")//Using alias
      .withColumnRenamed("outNode","midNode").join(edges,"midNode","inner")//Performing Inner Join
      .where("outNode != inNode")//Removing Hop Paths
//    path.explain()

    val triangles3: DataFrame = path.withColumnRenamed("outNode","join2Node")
      .join(edges.withColumnRenamed("midNode","join2Node"),("join2Node"), "inner")
      .where(("outNode == inNode"))
//    triangles3.explain()

    accum.add(triangles3.count())//incrementing accumulator count
    logger.info(s"Social Triangles Count found:${accum.value / 3}")//Logging into root logger
//    triangles3.write.format("csv").save(args(1))
  }
}