package com.iclick.tracking.main
import org.apache.commons.lang3.StringUtils
import scala.util.matching.Regex
import com.iclick.tracking.util.Models.Searchengine
import com.iclick.tracking.util.Models.GpLoadPretargeting
import com.iclick.tracking.util.Models
import scala.collection.mutable.ArrayBuffer
import com.buzzinate.common.util.ip.IPUtils
import com.iclick.spark.realtime.util.DomainNames
import com.iclick.tracking.util.Loggable
object RtbTracking extends Loggable  {

   def bshare_city() = {
    val gpload = new Models.GpLoadIPCity()
    gpload.load_bshare_city_mapping()
  }
   val bshare_city_load = bshare_city.cache()
  val bshare_city_mapping = bshare_city_load.collect().toMap[String, String]
  
  
  def serachine_info() = {
    val seraching_load = new Searchengine()
    seraching_load.load_searchengine() //id, searchengine, campaign_id ,client_id,hash_id_type
  }
  val seraching_load = serachine_info().cache()
  val seraching_load_mapping = seraching_load.collect().toMap

  def pretargeting() = {
    val pretargeting = new GpLoadPretargeting()
    pretargeting.load_searching_adgroup()
  }
  val pretargeting_load = pretargeting().cache()
  val pretargeting_mapping = pretargeting_load.collect().toMap

  def platform(opxplatform: String) = {
    if (StringUtils.isNotBlank(opxplatform)) {
      Some(opxplatform)
    } else {
      None
    }
  }

  def winning_price(opxwp: String) = {
    if (StringUtils.isNotBlank(opxwp)) {
      Some(opxwp)
    } else {
      None
    }

  }

  def bidded_at(opxdatetime: String) = {
    val result =
      viewed_at(opxdatetime)
    result
  }

  def client_id(opxseid: String) = {
    try {
      Some(seraching_load_mapping.get(opxseid.toInt).getOrElse("", "", "", "")._3)
    } catch {
      case e: Exception => None
    }
  }

  def pretargeting_id(opxseid: String) = {
    try {

      Some(pretargeting_mapping.getOrElse(opxseid, ""))

    } catch {
      case e: Exception => None
    }

  }

  def placement_url(opxpl: String) = {
    if (StringUtils.isNotBlank(opxpl)) {
      Some(opxpl)
    } else {
      None
    }
  }
  def cpm(opxwp: String) = {
    try {
      if (opxwp.toDouble > 0) {
        (opxwp.toDouble * 1000).toInt
      } else {
        0
      }

    } catch {
      case e: Exception => 0
    }
  }
  def ip(opxip:String)={
    if(StringUtils.isNotBlank(opxip)){
      Some(opxip)
    }else{
      None
    }
  }
  
   def ipcity(opxip: String) = {
    val regex = new Regex("""([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""")
    val result = opxip match {
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

   
   def  tracking_machine(machine:String)={
     if(StringUtils.isNotBlank(machine)){
       Some(machine)
     }else{
       None
     }
   }
  
  def  max_viewed_percent()={
    None
  }
  def opxaud(opxaud:String)={
    if(StringUtils.isNoneBlank(opxaud)){
      Some(opxaud)
    }else{
      None
    }
  }
  def  crm_hash(opxcrm:String)={
    if(StringUtils.isNotBlank(opxcrm)){
      Some(opxcrm)
    }else{
      None
    }
  }
  def domian(opxpl: String): Option[String] = {

    if (StringUtils.isNotBlank(opxpl)) {
      try {
        Some(DomainNames.safeGetHost(java.net.URLDecoder.decode(opxpl, "UTF-8")))

      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }
  def canonical(opxpl: String): Option[String] = {
    if (StringUtils.isNotBlank(opxpl)) {
      try {
        Some(DomainNames.safeGetPLD(java.net.URLDecoder.decode(opxpl, "UTF-8")))
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }
  
  
  
  def viewed_at(opxdatetime: String) = {
    if (StringUtils.isNotBlank(opxdatetime)) {
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      val sdf1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = sdf1.format(sdf.parse(opxdatetime))
      Some(time)
    } else {
      warn("rtb viewed_at is null")
      None
    }
  }
  def viewed_at_i(opxdatetime: String) = {
    if (StringUtils.isNotBlank(opxdatetime)) {
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val time = sdf.format(sdf.parse(opxdatetime)).replaceAll("-", "")
      Some(time)
    } else {
      warn("rtb viewed_at_i is null")
      None
    }
  }
  
  def  fraud_validate(fraud:String)={
    if(StringUtils.isNumeric(fraud) &&fraud.toInt==1) -32  else  0
  }

/*  def main(args: Array[String]): Unit = {
    println(fraud_validate("1"))
  }
  */
}