package com.iclick.tracking.main
import scala.util.matching.Regex
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.commons.lang.StringUtils
import java.net.URLDecoder
import com.iclick.spark.realtime.util.DomainNames
import com.buzzinate.common.util.ip.IPUtils
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SQLContext

object Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("newworkwordcont").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.streaming.kafka.maxRatePerPartition", "5")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._

 
    println("读取csv格式文件")

    val df_csv = sqlcontext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("d:\\wilson.zhou\\Desktop\\test_csv.csv")
    df_csv.show()

    val df = sc.parallelize(Seq((1, 2), (3, 4)), 2)
    val df1 = sc.parallelize(Seq((1, 2), (3, 4)), 2).toDF("a1", "a2")
    val df2 = sc.parallelize(Seq((8, 9)), 2).toDF("a3", "a4")
    val result = df1.unionAll(df2)
 
//println("写数据进csv中")
//     df_csv.write
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("newcars.csv")

    
  }

}