package com.iclick.tracking.main
import kafka.serializer.StringDecoder
import scala.util.matching.Regex
import com.iclick.tracking.util.InternalRedisClient
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
import scala.collection.mutable.HashMap
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang.StringUtils
import com.kafka.cluster.KafkaCluster
import com.kafka.cluster.KafkaManager
import com.iclick.spark.realtime.util.DomainNames
import com.buzzinate.common.util.ip.IPUtils
import com.iclick.tracking.util.Models
import com.iclick.tracking.util.Models.Searchengine
import scala.collection.mutable.ArrayBuffer
import com.iclick.tracking.util.Loggable
object AdgroupTracking extends Loggable {
  /*val maxTotal = 10
  val maxIdle = 10
  val minIdle = 1
  val redisHost = "127.0.0.1"
  val redisPort = 6379
  val redisTimeout = 30000
  val dbIndex = 1
  InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
  val jedis = InternalRedisClient.getPool.getResource
*/
  def bshare_city() = {
    val gpload = new Models.GpLoadIPCity()
    gpload.load_bshare_city_mapping()
  }
  def serachine_info() = {
    val seraching_load = new Searchengine()
    seraching_load.load_searchengine() //id, searchengine, campaign_id ,client_id,hash_id_type
  }

  val bshare_city_load = bshare_city.cache()
  val bshare_city_mapping = bshare_city_load.collect().toMap[String, String]
  val seraching_load = serachine_info().cache()
  val seraching_load_mapping = seraching_load.collect().toMap

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //    val conf = new SparkConf().setMaster("local[2]").setAppName("newworkwordcont").
    //      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    //      set("spark.streaming.kafka.maxRatePerPartition", "5")
    //    val sc = new SparkContext(conf)
    //
    //    val ssc = new StreamingContext(sc, Seconds(5))
    //    val topics = Set("wilson")
    //    val kafkaParm = Map("metadata.broker.list" -> "192.168.118.20:9092",
    //      "auto.offset.reset" -> "smallest", "group.id" -> "davidtopic")
    println(adtext_id("70648311"))
  }

  /*def ht_score(opxip: String, jedis: Jedis, dbIndex: Int): Option[Int] = {
    jedis.select(dbIndex)
    val regex = new Regex("""([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""")
    val result = opxip match {
      case regex(x) => try {
        Some(jedis.get("nhtip:" + x).toInt)
      } catch {
        case e: Exception => Some(0)
      }
      case _ => Some(0)
    }
    result
  }*/

  def device(devive: String, opxseid: Int, mobile: String, useragent: String): Option[String] = {
    val result = if (devive == "m") {
      Some("Mobile")
    } else if (devive == "t") {
      Some("Tablet")
    } else if (devive == "c") {
      Some("Computer")
    } else if (devive == "") {
      val searchengine_name = seraching_load_mapping.get(opxseid).getOrElse("", "", "", "")._1.toLowerCase
      if (searchengine_name == "google") {
        if (mobile != null) {
          if (mobile.toInt == 1) {
            Some("Mobile")
          } else {
            Some("Computer")
          }

        } else {
          Some("Mobile")
        }

      } else if (searchengine_name == "baidu" || searchengine_name == "bing") {

        Some("Mobile")
      } else {
        None
      }
    } else {
      None
    }

    result
  }

  def searchengine_id(opxseid: String): Option[Int] = {
    val regex = new Regex("""([0-9]+)""")
    val er = opxseid match {
      case regex(x) => Some(x.toInt)
      case _        => None
    }
    er
  }
  def browser_ip(opxip: String) = {
    if (StringUtils.isNotBlank(opxip)) {
      Some(opxip)
    } else {
      None
    }
  }

  def useragent(useragent: String): Option[String] = {
    if (StringUtils.isNotBlank(useragent)) {
      Some(useragent)
    } else {
      None
    }
  }

  def client_id(opxseid: String): Option[String] = {
    if (StringUtils.isNotBlank(opxseid)) {
      Some(seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._3.toString)
    } else {
      warn("adgrop client_id is null")
      None
    }
  }

  def ip(ip_test: String) = {
    val regex = new Regex("""([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""")
    val result = ip_test match {
      case regex(x) => Some(x)
      case _        => None
    }
    result
  }
  def ipcity(ip: String) = {
    val regex = new Regex("""([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""")
    val result = ip match {
      case regex(x) => {
        val bshare_code = IPUtils.getIpLocation(x).getCityCode()
        if (!StringUtils.isBlank(bshare_code)) {
          val ipcity = bshare_city_mapping.getOrElse(bshare_code, "")
          val arr = ArrayBuffer[String]()
          //          println(ipcity)
          arr += ipcity
          arr += "CHINA"
          arr += "CN"
          Option(arr)
        } else {
          val ipcity = try {
            IPUtils.getIpLocation(x).getProvinceCode()
          } catch {
            case e: Exception => ""
          }

          val arr = ArrayBuffer[String]()
          arr += ipcity; arr += ipcity; arr += ipcity
          Option(arr)
        }

      }
      case _ => None
    }
    result
  }

  def hash_id(keyword: String, matchtype: String, kwid: String, opxseid: String, opxagid: String) = {
    //    java.net.URLDecoder.decode(placement, "UTF-8")
    val result = if (StringUtils.isNotBlank(keyword) && StringUtils.isNotBlank(matchtype)) {
      Some("kw:" + matchtype.toString + ":" + java.net.URLDecoder.decode(keyword, "UTF-8"))
    } else if (StringUtils.isNotBlank(kwid)   && kwid.toInt > 0) {
      if (StringUtils.isNotBlank(opxseid) &&
        List("Yahoo", "Baidu", "Sogou", "Bing").contains(seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1) &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._3 == "new") {
        if (seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1 == "Bing" && matchtype == "c") {
          None
        } else {
          Some(kwid)
        }
      } else if (StringUtils.isNotBlank(opxseid) &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1 == "Google" &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._3 == "new") {

        Some(opxagid.toString + "|" + kwid)

      } else {
        Some("kwid:" + kwid)
      }
    } else {

      None

    }
    result
  }
  def keyword_key(keyword: String, matchtype: String, kwid: String, opxseid: String, opxagid: String) = {
    //    java.net.URLDecoder.decode(placement, "UTF-8")
    val result = if (StringUtils.isNotBlank(keyword) && StringUtils.isNotBlank(matchtype)) {
      Some("kw:" + matchtype.toString + ":" + java.net.URLDecoder.decode(keyword, "UTF-8"))
    } else if (StringUtils.isNotBlank(kwid)  && kwid.toInt > 0) {
      if (StringUtils.isNotBlank(opxseid) &&
        List("Yahoo", "Baidu", "Sogou", "Bing").contains(seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1) &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._3 == "new") {
        if (seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1 == "Bing" && matchtype == "c") {
          None
        } else {
          Some(kwid)
        }
      } else if (StringUtils.isNotBlank(opxseid) &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._1 == "Google" &&
        seraching_load_mapping.get(opxseid.toInt).getOrElse(("", "", "", ""))._3 == "new") {

        Some(opxagid.toString + "|" + kwid)

      } else {
        Some(kwid)
      }
    } else {

      None

    }

    result

  }

  def placement(placement: String): Option[String] = {
    if (StringUtils.isNotBlank(placement)) {
      Some(placement)
    } else {
      None
    }
  }
  //placement  domain
  def domian(placement: String): Option[String] = {

    if (StringUtils.isNotBlank(placement)) {
      try {
        Some(DomainNames.safeGetHost(java.net.URLDecoder.decode(placement, "UTF-8")))

      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }
  def canonical(placement: String): Option[String] = {
    if (StringUtils.isNotBlank(placement)) {
      try {
        Some(DomainNames.safeGetPLD(java.net.URLDecoder.decode(placement, "UTF-8")))
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }
  def adslot_id(slotid: String): Option[String] = {
    if (StringUtils.isNotBlank(slotid)) {
      Some(slotid)
    } else {
      None
    }
  }
  def crm_hash(opxcrm: String): Option[String] = {
    if (StringUtils.isNotBlank(opxcrm)) {
      Some(opxcrm)
    } else {
      None
    }
  }
  def aud_hash(opxaud: String): Option[String] = {
    if (StringUtils.isNotBlank(opxaud)) {
      Some(opxaud)
    } else {
      None
    }
  }

  def bid_request_id(opxbid: String): Option[String] = {
    if (StringUtils.isNotBlank(opxbid)) {
      Some(opxbid)
    } else {
      None
    }
  }

  def ismobile(mobile: String): Option[String] = {
    if (StringUtils.isNotBlank(mobile)) {
      Some(mobile)
    } else {
      None
    }
  }

  def opxsid(opxsid: String): Option[String] = {
    if (StringUtils.isNotBlank(opxsid)) {
      Some(opxsid)
    } else {
      None
    }
  }
  def opxpid(opxpid: String): Option[String] = {
    if (StringUtils.isNotBlank(opxpid)) {
      Some(opxpid)
    } else {
      warn("adgrop opxpid is null")
      None
    }
  }

  def click_referring_site(opxreferer: String): Option[String] = {
    if (StringUtils.isNotBlank(opxreferer)) {
      Some(opxreferer)
    } else {
      None
    }
  }
  def uuid(uuid: String): Option[String] = {
    if (StringUtils.isNotBlank(uuid)) {
      Some(uuid)
    } else {
      warn("adgrop uuid is null")
      None
    }
  }
  


 def referer(opxreferer: String): Option[String] = {
   if (StringUtils.isNotBlank(opxreferer)) {
      val leading_site = java.net.URLDecoder.decode(opxreferer, "UTF-8")
    
      var hostname_start_index = leading_site.indexOf("://")
      if (hostname_start_index.toInt > 0) {
        hostname_start_index += 3
      }
      var hostname_end_index = leading_site.indexOf("/", hostname_start_index.toInt)
      var split_index = leading_site.indexOf("?")
      val leading_host = if (hostname_end_index > 0) {
        leading_site.substring(0, hostname_end_index)
      } else {
       
          if(split_index>0){
        leading_site.substring(0, split_index)
        }else{
          leading_site
        }
      }

      val leading_query = if (split_index < 0) {
        null
      } else {
        leading_site.substring(split_index, leading_site.length - 1)
      }

      if (leading_query == null) {
        return Some(leading_host + "," + leading_query)
      } else {

        val query_array = leading_query.replace("?", "&").split("&")
        val map = new HashMap[String, String]()
        for (arr <- query_array) {
         
          val key_parir = arr.split("=")
          if(key_parir.length>1)
            map.put(key_parir.apply(0), key_parir.apply(1))
        }
    

        if (leading_host.toLowerCase.indexOf("google.com") > 0 & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("q").get,"UTF-8"))
          } else if (map.get("as_q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("as_q").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }

        } else if (leading_host.toLowerCase.indexOf("yahoo.com") > 0 & leading_query.indexOf("p=") > 0) {

          if (map.get("p") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("p").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("bing.com") > 0 & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("q").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("baidu.com") > 0 & (leading_query.indexOf("wd=") > 0 | leading_query.indexOf("word=") > 0)) {
           
          if (map.get("wd") != None) {
           
            return Some(leading_host + "," +java.net.URLDecoder.decode(map.get("wd").get,"UTF-8"))
          } else if (map.get("word") != None) {
            
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("word").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("sogou.com") > 0 & leading_query.indexOf("query=") > 0) {
          if (map.get("query") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("query").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if ((leading_host.toLowerCase.indexOf("360.cn") > 0 | leading_query.toLowerCase.indexOf("so.com") > 0) & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," +java.net.URLDecoder.decode(map.get("q").get,"UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else {
          return Some(leading_host + "," + " ")
        }

      }
    } else {
      None
    }
  }
  def leading_site(opxreferer: String) = {
    referer(opxreferer).getOrElse(" , ").split(",").apply(0)
  }

  def leading_keyword(opxreferer: String) = {
    referer(opxreferer).getOrElse(" , ").split(",").apply(1)
  }

  def adgroup_id(opxagid: String): Option[Int] = {
    val regex = new Regex("([0-9]+)")
    val result = opxagid match {
      case regex(x) => if (x.toInt > 0) Some(x.toInt) else None
      case _        => None
    }
    result

  }
  def adtext_id(creative: String): Option[Long] = {
    val regex = new Regex("([0-9]+)")
    creative match {
      case regex(x) => if (x.toLong > 0) Some(x.toLong) else None
      case _        => None
    }
  }

  def click_date(opxdatetime: String): Option[String] = {
   
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (StringUtils.isNotBlank(opxdatetime)) {
      try {
        Some(sdf1.format(sdf.parse(opxdatetime)))
      } catch {
        
        case e: Exception => warn("adgrop click_date is null");None
      }
    } else {
      warn("adgrop click_date is null")
      None
    }

  }

  def robot_filter_flag() = {
    1
  }
  def ip_filter_flag() = {
    1
  }

  def c_date(opxdatetime: String): Option[String] = {
    //    click_date ? click_date.strftime("%Y%m%d").to_i : nil
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    if (StringUtils.isNotBlank(opxdatetime)) {
      try {
        Some(sdf.format(sdf.parse(opxdatetime)).replaceAll("-", ""))
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }

  }

  def tagid(tagId: String): Option[String] = {
    if (StringUtils.isNotBlank(tagId)) {
      Some(tagId)
    } else {
      None
    }

  }

  /*def fraud_validate(fraud: String) = {
    //    fraud = input["query_hash"]["fraud"].to_i   //    mark_invalid 32 if fraud == 1
    val fra = fraud.toInt
    if (fra == 1) {
      valid_flag = 30
      valid_flag
    } else {
      valid_flag
    }

  }*/
  
    def  fraud_validate(fraud:String)={
    if(StringUtils.isNumeric(fraud) &&fraud.toInt==1) -32  else  0
  }

 
  def adx_name(opxadx: String): Option[String] = {
    if (StringUtils.isNotBlank(opxadx)) {
      Some(opxadx)
    } else {
      None
    }
  }

}