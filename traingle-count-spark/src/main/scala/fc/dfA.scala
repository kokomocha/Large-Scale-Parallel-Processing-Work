package fc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager

object dfA {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Arguments Error!\nCorrect Usage: <input dir> <output dir>")
      System.exit(1)
    }

    //Using SparkSession to aid reading as DataFrame
    //SQLContext deprecated
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("dataFrame Aggregation Program").getOrCreate()

    //Main
    val schema = new StructType()
      .add("InNode",IntegerType,true)//Generated schema for reading as DataFrame
      .add("OutNode",IntegerType,false)//Nullable is False(Think as Primary key)
    val df= spark.read.format("CSV").schema(schema).load(args(0))

    val count = df.filter("OutNode % 100 = 0").groupBy("OutNode").count()
//    count.explain()
    count.write.format("csv").save(args(1))

    spark.stop()
  }
}