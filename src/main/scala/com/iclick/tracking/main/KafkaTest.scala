package com.iclick.tracking.main
import kafka.serializer.StringDecoder
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import kafka.common.TopicAndPartition
import com.alibaba.fastjson.JSON
import com.kafka.cluster.KafkaCluster
import com.kafka.cluster.KafkaManager
import com.iclick.tracking.util.DBManageSer
import java.sql.{ DriverManager, PreparedStatement, Connection }
object KafkaTest {
  def processRdd(rdd: RDD[(String, String)]) = {
    val lines = rdd.map(_._2).map {
      x =>
        try {
          var result = JSON.parseObject(x)
          (result.get("type").toString.toInt, result.get("campaign").toString())
        } catch {
          case t: Exception => (0, "null") // TODO: handle error
        }
    }
    lines.foreach(println)
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("newworkwordcont").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.streaming.kafka.maxRatePerPartition", "5")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint(".")   // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    /*  val topics = Set("wilson")
      val kafkaParm = Map("metadata.broker.list" -> "192.168.118.20:9092",
      "auto.offset.reset" -> "smallest", "group.id" -> "davidtopic")*/
    val topics = Set("mytopic")
    val kafkaParm = Map("metadata.broker.list" -> "localhost:9092",
      "auto.offset.reset" -> "smallest", "group.id" -> "davidtopic")

    //"auto.offset.reset"->"smallest" 
    val kc = new KafkaCluster(kafkaParm)

    val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
      val curr = currentValues.sum
      val pre = preValue.getOrElse(0)
      Some(curr + pre)
    }

    val km = new KafkaManager(kafkaParm)
    val kafkaStrem = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, topics)
    val path = System.currentTimeMillis().toString()
    //     kafkaStrem.map(_._2).map(x=>(x,1)).reduceByKey(_+_)

    //    kafkaStrem.map(_._2).map(x => (x, 1)).reduceByKey(_ + _).repartition(1).saveAsTextFiles("D:\\tmp\\wilson\\test")
//    val con = DBManageSer.getConnect.get
//    val state = con.createStatement()
    kafkaStrem.foreachRDD {
      rdd =>
        if (rdd.count() > 0) {
          val tras = rdd.map(_._2).map(x => (x, 1)).reduceByKey(_ + _)
          println("tras的数量"+tras.count())
          tras.foreachPartition { part =>  
            rddtomysql(part)
          }
        }
        km.updateZKOffsets(rdd)

    }
//    DBManageSer.closecon(con)

    ssc.start() //真正的启动
    ssc.awaitTermination() //阻塞等待
  }

  def rddtomysql(iteror: Iterator[(String, Int)]) {
    var conn: Connection = DBManageSer.getConnect.get
    val sql = "insert into wordsegment(name,id,age) values(?,?,?)"
    var ps: PreparedStatement = null

    try {
      iteror.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2.toString)
        ps.setInt(3, 10000)
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("mysql exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

}