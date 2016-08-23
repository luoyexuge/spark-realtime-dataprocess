package com.iclick.tracking.util
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast
import com.mysql.jdbc.Driver
import org.postgresql.Driver
import scala.collection.mutable.Map
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang.StringUtils
import java.util.Properties
object Models extends Loggable{
  
  //local
  /*val conf = new SparkConf().setAppName("spark stream test").setMaster("local[4]").
      set("spark.sql.shuffle.partitions", "10").set("spark.network.timeout", "30s").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.streaming.kafka.maxRatePerPartition", "5")*/
  
//  master
  val conf = new SparkConf().setAppName("spark stream ").set("spark.sql.shuffle.partitions", "10").set("spark.network.timeout", "30s").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.streaming.kafka.maxRatePerPartition", "5")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val pro = new java.util.Properties
  pro.setProperty("user", "xmo_prd")
  pro.setProperty("password", "MWc200bs")
  pro.setProperty("use_unicode", "true")
  pro.setProperty("characterEncoding", "utf8")
  pro.setProperty("url", "jdbc:mysql://10.1.2.200:3306/xmo")
  pro.setProperty("fetchSize", "4")

  val url1 = "jdbc:postgresql://10.1.1.230:5432/xmo_dw"
  val pro1 = new java.util.Properties
  pro1.setProperty("user", "david_xu")
  pro1.setProperty("password", "w7dtfxHD")
  pro1.setProperty("url", url1)
  //这里必须有个别名否认会报错

  class Searchengine {
    def load_searchengine() = {
      val map = Map[String, String]()
      map ++= pro.asScala
      map.put("dbtable", """(SELECT id, searchengine, campaign_id ,client_id,hash_id_type  FROM xmo.searchengines ) as result""")
      //      println(map)
      val jdbcDF = sqlContext.read.format("jdbc").options(map).load()

      jdbcDF.rdd.map {
        x =>
          (x.apply(0).toString.toInt, (x.apply(1).toString, x.apply(2).toString, x.apply(3).toString, x.apply(4).toString))
      }

    }
  }

  class GpLoadIPCity {
    def load_bshare_city_mapping() = {
      val map = Map[String, String]()
      map ++= pro1.asScala
      map.put("dbtable", "( select  *  from  xmo_dw.bshare_city_mapping) as result")
      val jdbcDF = sqlContext.read.format("jdbc").options(map).load()
      jdbcDF.rdd.map { x =>
        (x.apply(1).toString.trim, x.apply(0).toString.toUpperCase.trim)
      }
    }

  }

  class GpLoadPretargeting {
    def load_searching_adgroup() = {
      val map = Map[String, String]()
      map ++= pro1.asScala
      map.put("dbtable", "( select searchengine_id,adgroup_id from xmo_dw.adgroups ) as result")
      val jdbcDF = sqlContext.read.format("jdbc").options(map).load()
      jdbcDF.rdd.map { x =>
        (x.apply(0).toString.trim, x.apply(1).toString.toUpperCase.trim)
      }
    }

  }
  class Creatives {
    def load_creatives() = {

      val map = Map[String, String]()
      map ++= pro.asScala
      map.put("dbtable", """(select  id ,campaign_id ,client_id from    xmo.creatives where id  is not null ) as  result""")
      val jdbcDF = sqlContext.read.format("jdbc").options(map).load

      jdbcDF.rdd.map {
        x =>

          val key = x.apply(0).toString.toInt
          val value1 = try { x.apply(1).toString.toInt }
          catch {
            case t: Exception => 0
          }
          val value2 = try { x.apply(2).toString.toInt }
          catch {
            case t: Exception => 0
          }
          (key, (value1, value2))

      }
    }

  }
  class Event {
    def load_eventid()={
      val map = Map[String, String]()
      map ++= pro.asScala

      map.put("dbtable", """( SELECT  id,event_one_name,event_two_name,event_three_name,
       event_four_name,event_five_name,event_six_name from eventinfos where id is not null) as result""")
      val jdbcDF = sqlContext.read.format("jdbc").options(map).load

      val result = jdbcDF.rdd.map {
        x =>
          val key = x.apply(0).toString.toInt

          val event_one = try {
            "opx" + x.apply(1).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }

          val event_two = try {
            "opx" + x.apply(2).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }

          val event_three = try {
            "opx" + x.apply(3).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }
          val event_four = try {
            "opx" + x.apply(4).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }
          val event_five = try {
            "opx" + x.apply(5).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }
          val event_six = try {
            "opx" + x.apply(6).toString.toLowerCase
          } catch {
            case e: Exception => "notexist"
          }

          (key, (event_one, event_two, event_three, event_four, event_five, event_six))
      }
      result
    }

  }

  def main(args: Array[String]): Unit = {
      val pro=Config.getConfig("config.properties")
    val ser = new Models.Event

    val sdsd = ser.load_eventid().cache()
    sdsd.toDF.show() 
    info("zhoumeixu")
  }

}