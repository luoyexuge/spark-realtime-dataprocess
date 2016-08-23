package com.iclick.tracking.main
import scala.util.matching.Regex
import org.apache.commons.lang.StringUtils
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import com.buzzinate.common.util.ip.IPUtils
import com.iclick.tracking.util.Models
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import com.alibaba.fastjson.JSON
import com.iclick.tracking.util.Loggable

object EventTracking extends Loggable {

  val bsharecity = new Models.GpLoadIPCity()
  val bsharecity_rdd = bsharecity.load_bshare_city_mapping().cache()
  val bshare_city_mapping = bsharecity_rdd.collect().toMap

  val envent_type = new Models.Event()
  val envent_type_rdd = envent_type.load_eventid().cache()
  val envent_type_mapping = envent_type_rdd.collect().toMap

  /*Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val conf = new SparkConf().setMaster("local[2]").setAppName("sdsd")
  val sc = new SparkContext(conf)*/

  def mobile_ip_get() = {

    val map = new HashMap[String, String]()
    //    val read = scala.io.Source.fromFile("./src/main/resources/mobile_ip.txt").getLines()
    val read = scala.io.Source.fromFile("mobile_ip.txt").getLines()
    for (i <- read) {
      val json = JSON.parseObject(i)
      map.put(json.getString("baseip"), json.getString("type"))
    }
    map
  }

  val mobile_ip = Models.sc.broadcast(mobile_ip_get())

  def event_id(opxeventid: String) = {
    val regex = new Regex("^([0-9]+)$")
    val result = opxeventid match {
      case regex(x) => x.toInt
      case _        => 0
    }
    result
  }

  def event_one(event_id: Int) = {
    val result=
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._1
    if(result=="notexist"){
      warn("event event_one is null")
    }
    result
  }
  def event_two(event_id: Int) = {
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._2
  }
  def event_three(event_id: Int) = {
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._3
  }

  def event_four(event_id: Int) = {
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._4
  }
  def event_five(event_id: Int) = {
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._5
  }
  def event_six(event_id: Int) = {
    envent_type_mapping.getOrElse(event_id, ("notexist", "notexist", "notexist", "notexist", "notexist", "notexist"))._6
  }

  def client_id(opxclientid: String) = {

    val regex = new Regex("^([0-9]+)$")
    val result = opxclientid match {
      case regex(x) => x.toInt
      case _        => 0
    }
    result

  }

  def ip(opxip: String, ipsd: String) = {
    val regex = new Regex("([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)")

    if (StringUtils.isNotBlank(opxip)) {

      opxip match {
        case regex(x) => x
        case _        => ""
      }

    } else if (StringUtils.isNotBlank(ipsd)) {
      ipsd match {
        case regex(x) => x
        case _        => ""
      }
    } else {
      ""
    }
  }
  def mobile_tag(iptest: String) = {
    mobile_ip.value.getOrElse(iptest, "")
  }

  def ip_8b(result: String) = {

    if (StringUtils.isNotBlank(result)) {
      result.split("\\.").apply(0)
    } else {
      ""
    }
  }

  def ip_16b(result: String) = {

    if (StringUtils.isNotBlank(result)) {
      result.split("\\.").take(2).mkString(".")
    } else {
      ""
    }

  }

  def ip_24b(result: String) = {

    if (StringUtils.isNotBlank(result)) {
      result.split("\\.").take(3).mkString(".")
    } else {
      ""
    }

  }

  def main(args: Array[String]): Unit = {
    println(ip("11.12.1.2", "192.16.1.12"))

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

  def event_date(opxdate: String, date: String) = {
    val result = if (StringUtils.isNotBlank(opxdate)) {

      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      try {
        Some(sdf.format(sdf1.parse(opxdate)))
      } catch {
        case e: Exception =>warn("event event_date is null"); None
      }

    } else if (StringUtils.isNotBlank(date)) {
      val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      try {
        Some(sdf.format(sdf1.parse(date)))
      } catch {
        case e: Exception =>warn("event event_date is null"); None
      }

    } else {
      warn("event event_date is null")
      None

    }

    result
  }

  def e_date(opxdate: String, date: String) = {
    val result = if (StringUtils.isNotBlank(opxdate)) {
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyyMMdd")
      try {
        Some(sdf1.format(sdf.parse(opxdate)))
      } catch {
        case e: Exception => None
      }

    } else if (StringUtils.isNotBlank(date)) {
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyyMMdd")
      try {
        Some(sdf1.format(sdf.parse(date)))
      } catch {
        case e: Exception => None
      }

    } else {

      None

    }

    result
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
        if (split_index > 0) {
          leading_site.substring(0, split_index)
        } else {
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
          if (key_parir.length > 1)
            map.put(key_parir.apply(0), key_parir.apply(1))
        }

        if (leading_host.toLowerCase.indexOf("google.com") > 0 & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("q").get, "UTF-8"))
          } else if (map.get("as_q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("as_q").get, "UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }

        } else if (leading_host.toLowerCase.indexOf("yahoo.com") > 0 & leading_query.indexOf("p=") > 0) {

          if (map.get("p") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("p").get, "UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("bing.com") > 0 & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("q").get, "UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("baidu.com") > 0 & (leading_query.indexOf("wd=") > 0 | leading_query.indexOf("word=") > 0)) {

          if (map.get("wd") != None) {

            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("wd").get, "UTF-8"))
          } else if (map.get("word") != None) {

            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("word").get, "UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if (leading_host.toLowerCase.indexOf("sogou.com") > 0 & leading_query.indexOf("query=") > 0) {
          if (map.get("query") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("query").get, "UTF-8"))
          } else {
            return Some(leading_host + "," + " ")
          }
        } else if ((leading_host.toLowerCase.indexOf("360.cn") > 0 | leading_query.toLowerCase.indexOf("so.com") > 0) & leading_query.indexOf("q=") > 0) {
          if (map.get("q") != None) {
            return Some(leading_host + "," + java.net.URLDecoder.decode(map.get("q").get, "UTF-8"))
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

  def source_channel(frompaidsearch: String, opxreferer: String) = {
    if (StringUtils.isNumeric(frompaidsearch) && frompaidsearch.toInt == 1) {
      "Paid Search"
    } else {
      if (StringUtils.isNotBlank(leading_keyword(opxreferer))) {
        "Organic"
      } else {
        "Direct"
      }
    }
  }

}