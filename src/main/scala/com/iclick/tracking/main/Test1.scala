package com.iclick.tracking.main
import scala.collection.mutable.Map
import kafka.serializer.StringDecoder
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import scala.io.Source
import org.apache.commons.lang.StringUtils
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkFiles
object Test1 {

  def test(list: List[String], f: String => String) = {
    val arr=ListBuffer[String]()
    list.foreach { x =>
    val temp=f(x)
    arr.append(temp)
    }
    arr

  }

  def main(args: Array[String]): Unit = {
   Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
   val conf = new SparkConf().setAppName("spark stream test").setMaster("local[4]")
  val sc = new SparkContext(conf)
     val  path="d:\\wilson.zhou\\Desktop\\test1.txt"
    sc.addFile(path)
    val rdd=sc.textFile(SparkFiles.get(path))
     rdd.foreach(println)
    

  }
}