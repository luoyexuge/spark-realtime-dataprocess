package com.iclick.tracking.main
import org.apache.commons.lang.StringUtils
import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import com.iclick.tracking.util.Models
import scala.collection.mutable.ArrayBuffer
import com.buzzinate.common.util.ip.IPUtils
import com.iclick.tracking.util.Loggable
object ClickTracking extends Loggable {

  val seraching_load = new Models.Searchengine() //id, searchengine, campaign_id ,client_id,hash_id_type
  val seraching_load_rdd = seraching_load.load_searchengine().cache()
  val seraching_load_mapping = seraching_load_rdd.collect().toMap

  val bsharecity = new Models.GpLoadIPCity()
  val bsharecity_rdd = bsharecity.load_bshare_city_mapping().cache()
  val bshare_city_mapping = bsharecity_rdd.collect().toMap

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

  def searchengine_id(opxseid: String): Option[Int] = {
    val regex = new Regex("""([0-9]+)""")
    val er = opxseid match {
      case regex(x) => Some(x.toInt)
      case _        => None
    }
    er
  }

  def client_id(opxseid: String) = {
    val searchengine = searchengine_id(opxseid).getOrElse(0)
    seraching_load_mapping.get(searchengine).getOrElse(("0", "0", "", ""))._3
  }

  def click_date(opxdate: String, date: String) = {
    val result = if (StringUtils.isNotBlank(opxdate)) {

      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      try {
        Some(sdf.format(sdf1.parse(opxdate)))
      } catch {
       
        case e: Exception => warn("click log click_date is null"); None
      }

    } else if (StringUtils.isNotBlank(date)) {
      val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      try {
        Some(sdf.format(sdf1.parse(date)))
      } catch {
        case e: Exception =>warn("click log click_date is null"); None
      }

    } else {
      warn("click log click_date is null")
      None

    }

    result
  }

  def c_date(opxdate: String, date: String) = {
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

}