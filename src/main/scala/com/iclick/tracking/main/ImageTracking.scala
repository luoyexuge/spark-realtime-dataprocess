package com.iclick.tracking.main
import org.apache.commons.lang.StringUtils
import com.iclick.tracking.util.Models
import com.buzzinate.common.util.ip.IPUtils
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import com.iclick.tracking.util.Loggable

object ImageTracking  extends  Loggable{
  val creatives = new Models.Creatives()
  val creatives_rdd = creatives.load_creatives().cache()
  val creatives_map = creatives_rdd.collect().toMap

  val bsharecity = new Models.GpLoadIPCity()
  val bsharecity_rdd = bsharecity.load_bshare_city_mapping().cache()
  val bshare_city_mapping = bsharecity_rdd.collect().toMap

  def creative_id(opxcreativeid: String, opxctid: String) = {
    val result = if (StringUtils.isNumeric(opxcreativeid)) {
      opxcreativeid.toInt
    } else if (StringUtils.isNumeric(opxctid)) {
      opxctid.toInt
    } else {
      -1
    }

    result
  }

  def campaign_id(opxcreativeid: String, opxctid: String) = {
    val creative = creative_id(opxcreativeid, opxctid)

    val result = creatives_map.getOrElse(creative, (0, 0))._1
    if(result==0){
      warn("image campaign_id is null")
    }

    result

  }

  def client_id(opxcreativeid: String, opxctid: String) = {
    val client = creative_id(opxcreativeid, opxctid)
    val result = creatives_map.getOrElse(client, (0, 0))._2
     if(result==0){
      warn("image client_id is null")
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

  def displayimage_id(opxcreativeassetid: String, opximageid: String, opxcaid: String) = {
    val result = if (StringUtils.isNumeric(opxcreativeassetid)) {
      opxcreativeassetid.toInt
    } else if (StringUtils.isNumeric(opximageid)) {
      opximageid.toInt
    } else if (StringUtils.isNumeric(opxcaid)) {
      opxcaid.toInt
    } else {
      -1
    }
    result
  }

  def placement_id(opxplacementid: String, opxplid: String) = {
    val result = if (StringUtils.isNumeric(opxplacementid)) {
      opxplacementid.toInt
    } else if (StringUtils.isNumeric(opxplid)) {
      opxplid.toInt
    } else {
      -1
    }
    result
  }

  def date(opxdatetime: String) = {
    if (StringUtils.isNotBlank(opxdatetime)) {

      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      try {
        Some(sdf.format(sdf.parse(opxdatetime)))
      } catch {
        case e: Exception => None
      }

    } else {

      None

    }

  }
  def date_i(opxdatetime: String) = {
    if (StringUtils.isNotBlank(opxdatetime)) {

      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyyMMdd")
      try {
        Some(sdf1.format(sdf.parse(opxdatetime)))
      } catch {
        case e: Exception => None
      }

    } else {

      None

    }

  }

  def fraud_validate(fraud: String) = {
    if (StringUtils.isNumeric(fraud) && fraud.toInt == 1) -32 else 0
  }

  def main(args: Array[String]): Unit = {
    println(StringUtils.isNumeric("123"))
    println(creative_id("13", "12"))
  }

}