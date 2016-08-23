package com.iclick.tracking.main
import kafka.serializer.StringDecoder
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import java.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.util.Random
import scala.io.Source
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang.StringUtils
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import com.kafka.cluster.KafkaCluster
import com.kafka.cluster.KafkaManager
import com.iclick.tracking.util.Loggable
import com.iclick.tracking.util.Models
import com.iclick.tracking.util.Config
import com.iclick.tracking.sendmail.SendMail
import com.iclick.tracking.util.WriterErrorData
object Tracking extends Loggable {
  val pro = Config.getConfig("config.properties")
  val rtb_save_path = pro.getString("")
  val adgroup_sava_path = pro.getString("")
  val click_save_path = pro.getString("")
  val image_save_path = pro.getString("")
  val event_save_path = pro.getString("")

  val rtb_save_path_error = pro.getString("")
  val adgroup_sava_path_error = pro.getString("")
  val click_save_path_error = pro.getString("")
  val image_save_path_error = pro.getString("")
  val event_save_path_error = pro.getString("")
  
  val kafka_host=pro.getString("kafka_host")
  val group_id=pro.getString("group_id")

  def clicktrackinglog(log: String) = {
    try {
      val logsplit = log.split("\t").apply(2)
      val parse = JSON.parseObject(logsplit)
      val ipcityconuntry = ClickTracking.ipcity(parse.getJSONObject("classic_payload").getString("opxip")).getOrElse(ArrayBuffer("", "", ""))

      val uuid = if (StringUtils.isNotBlank(parse.getString("uuid"))) parse.getString("uuid") else ""
      val click_date = ClickTracking.click_date(parse.getJSONObject("classic_payload").getString("opxdate"), parse.getJSONObject("classic_payload").getString("date")).getOrElse("")
      val click_referring_site = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxreferer"))) parse.getJSONObject("classic_payload").getString("opxreferer") else ""
      val hash_id = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxid"))) parse.getJSONObject("classic_payload").getString("opxid") else ""
      val opxpid = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxpid"))) parse.getJSONObject("classic_payload").getString("opxpid") else ""
      val opxsid = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxsid"))) parse.getJSONObject("classic_payload").getString("opxsid") else ""
      val searchengine_id = ClickTracking.searchengine_id(parse.getJSONObject("classic_payload").getString("opxseid")).getOrElse(0)
      val c_date = ClickTracking.c_date(parse.getJSONObject("classic_payload").getString("opxdate"), parse.getJSONObject("classic_payload").getString("date")).getOrElse("")
      val adtext_id = ""
      val adgroup_id = ""
      val placement = ""
      val leading_keyword = ClickTracking.leading_keyword(parse.getJSONObject("classic_payload").getString("opxreferer"))
      val leading_site = ClickTracking.leading_site(parse.getJSONObject("classic_payload").getString("opxreferer"))
      val ismobile = 0
      val keyword_key = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxid"))) parse.getJSONObject("classic_payload").getString("opxid") else ""
      val client_id = ClickTracking.client_id(parse.getJSONObject("classic_payload").getString("opxseid"))
      val useragent = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("useragent"))) parse.getJSONObject("classic_payload").getString("useragent") else ""
      val ip = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxip"))) parse.getJSONObject("classic_payload").getString("opxip") else ""
      val countrylong = ipcityconuntry.apply(2)
      val countryshort = ipcityconuntry.apply(1)
      val ipcity = ipcityconuntry.apply(0)
      val tracking_machine = if (StringUtils.isNotBlank(parse.getString("machine"))) parse.getString("machine") else ""
      val ip_filter_flag = 1
      val robot_filter_flag = 1
      val valid_flag = 1
      val ht_score = 101

      val result = uuid + "\t" + click_date + "\t" + click_referring_site + "\t" + hash_id + "\t" + opxpid + "\t" + opxsid + "\t" + searchengine_id + "\t" + c_date + "\t" + adtext_id + "\t" + adgroup_id +
        "\t" + placement + "\t" + leading_keyword + "\t" + leading_site + "\t" + ismobile + "\t" + keyword_key + "\t" + client_id + "\t" + useragent + "\t" + ip + "\t" + countrylong + "\t" +
        countryshort + "\t" + ipcity + "\t" + tracking_machine + "\t" + ip_filter_flag + "\t" + robot_filter_flag + "\t" + valid_flag + "\t" + ht_score

      if (StringUtils.isBlank(uuid) || StringUtils.isBlank(opxpid) || StringUtils.isBlank(client_id)) {

        WriterErrorData.saveErrorData(click_save_path_error, result)
        None
      } else {
        Some(result)
      }

    } catch {
      case e: Exception => SendMail.mail("click tracking error occurred", e.toString()); None
    }

  }

  def imagetrackinglog(log: String) = {
    try {
      val logsplit = log.split("\t").apply(2)
      val parse = JSON.parseObject(logsplit)
      val ipcityconuntry = ImageTracking.ipcity(parse.getJSONObject("query_hash").getString("opxip")).getOrElse(ArrayBuffer("", "", ""))

      val uuid = if (StringUtils.isNotBlank(parse.getString("uuid"))) parse.getString("uuid") else ""
      val creative_id = ImageTracking.creative_id(parse.getJSONObject("query_hash").getString("opxcreativeid"), parse.getJSONObject("query_hash").getString("opxctid"))
      val source = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxreferrer"))) parse.getJSONObject("query_hash").getString("opxreferrer") else ""
      val date = ImageTracking.date(parse.getJSONObject("query_hash").getString("opxdatetime")).getOrElse("")
      val date_i = ImageTracking.date_i(parse.getJSONObject("query_hash").getString("opxdatetime")).getOrElse("")
      val referring_site = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("refering_site"))) parse.getJSONObject("query_hash").getString("refering_site") else ""
      val opxsid = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxsid"))) parse.getJSONObject("query_hash").getString("opxsid") else ""
      val opxpid = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxpid"))) parse.getJSONObject("query_hash").getString("opxpid") else ""
      val view_type = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxtype"))) parse.getJSONObject("query_hash").getString("opxtype") else ""
      val displayimage_id = ImageTracking.displayimage_id(parse.getJSONObject("query_hash").getString("opxcreativeassetid"), parse.getJSONObject("query_hash").getString("opximageid"), parse.getJSONObject("query_hash").getString("opxcaid"))
      val placement_id = ImageTracking.placement_id(parse.getJSONObject("query_hash").getString("opxplacementid"), parse.getJSONObject("query_hash").getString("opxplid"))
      val campaign_id = ImageTracking.campaign_id(parse.getJSONObject("query_hash").getString("opxcreativeid"), parse.getJSONObject("query_hash").getString("opxctid"))
      val client_id = ImageTracking.client_id(parse.getJSONObject("query_hash").getString("opxcreativeid"), parse.getJSONObject("query_hash").getString("opxctid"))
      val email = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxmail"))) parse.getJSONObject("query_hash").getString("opxmail") else ""
      val useragent = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("useragent"))) parse.getJSONObject("query_hash").getString("useragent") else ""
      val ip = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxip"))) parse.getJSONObject("query_hash").getString("opxip") else ""
      val countrylong = ipcityconuntry.apply(2)
      val countryshort = ipcityconuntry.apply(1)
      val ipcity = ipcityconuntry.apply(0)
      val tracking_machine = if (StringUtils.isNotBlank(parse.getString("machine"))) parse.getString("machine") else ""
      val ip_filter_flag = 1
      val robot_filter_flag = 1
      val valid_flag = ImageTracking.fraud_validate(parse.getJSONObject("query_hash").getString("fraud"))
      val ht_score = 101
      val interest = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxinterest"))) parse.getJSONObject("query_hash").getString("opxinterest") else ""
      val age = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxage"))) parse.getJSONObject("query_hash").getString("opxage") else ""
      val gender = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxgender"))) parse.getJSONObject("query_hash").getString("opxgender") else ""
      val bsf = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("bsf"))) parse.getJSONObject("query_hash").getString("bsf") else ""
      val tdsid = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("opxtdsid"))) parse.getJSONObject("query_hash").getString("opxtdsid") else ""
      val tagid = if (StringUtils.isNotBlank(parse.getJSONObject("query_hash").getString("tagId"))) parse.getJSONObject("query_hash").getString("tagId") else ""
      val result = uuid + "\t" + creative_id + "\t" + source + "\t" + date + "\t" + date_i + "\t" + referring_site + "\t" + opxsid + "\t" + opxpid + "\t" + view_type + "\t" + displayimage_id + "\t" + placement_id + "\t" +
        campaign_id + "\t" + client_id + "\t" + email + "\t" + useragent + "\t" + ip + "\t" + countrylong + "\t" + countryshort + "\t" + ipcity + "\t" + tracking_machine + "\t" + ip_filter_flag + "\t" + robot_filter_flag + "\t" +
        valid_flag + "\t" + ht_score + "\t" + interest + "\t" + age + "\t" + gender + "\t" + bsf + "\t" + tdsid + "\t" + tagid

      if (StringUtils.isBlank(uuid) || client_id == 0 || campaign_id == 0) {
        WriterErrorData.saveErrorData(image_save_path_error, result)
        None

      } else {
        Some(result)
      }

    } catch {
      case e: Exception => SendMail.mail("image tracking error occurred", e.toString()); None
    }

  }

  def rtbtrackinglog(log: String) = {
    //    val logsplit = log.split("\\s{2,}").apply(2)

    try {
      val logsplit = log.split("\t").apply(2)
      val parese = JSON.parseObject(logsplit)
      val ipcityconuntry = RtbTracking.ipcity(parese.getJSONObject("rtb_hash").getString("opxip")).getOrElse(ArrayBuffer("", "", ""))

      val uuid = parese.getString("uuid")
      val bidded_at = RtbTracking.bidded_at(parese.getJSONObject("rtb_hash").getString("opxdatetime")).getOrElse("")
      val viewed_at = RtbTracking.viewed_at(parese.getJSONObject("rtb_hash").getString("opxdatetime")).getOrElse("")
      val viewed_at_i = RtbTracking.viewed_at_i(parese.getJSONObject("rtb_hash").getString("opxdatetime")).getOrElse("")
      val opxpid = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxpid"))) parese.getJSONObject("rtb_hash").getString("opxpid") else ""
      val opxsid = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxsid"))) parese.getJSONObject("rtb_hash").getString("opxsid") else ""
      val opxbid = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxbid"))) parese.getJSONObject("rtb_hash").getString("opxbid") else ""
      val client_id = RtbTracking.client_id(parese.getJSONObject("rtb_hash").getString("opxseid")).getOrElse("")
      val pretargeting_id = RtbTracking.pretargeting_id(parese.getJSONObject("rtb_hash").getString("opxseid")).getOrElse("")
      val searchengine_id = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxseid"))) parese.getJSONObject("rtb_hash").getString("opxseid") else ""
      val adgroup_id = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxag"))) parese.getJSONObject("rtb_hash").getString("opxag") else ""
      val adtext_id = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxad"))) parese.getJSONObject("rtb_hash").getString("opxad") else ""
      val placement_url = RtbTracking.placement_url(parese.getJSONObject("rtb_hash").getString("opxpl")).getOrElse("")
      val winning_price = RtbTracking.winning_price(parese.getJSONObject("rtb_hash").getString("opxwp")).getOrElse("")
      val cpm = RtbTracking.cpm(parese.getJSONObject("rtb_hash").getString("opxwp"))
      val ip = RtbTracking.ip(parese.getJSONObject("rtb_hash").getString("opxip")).getOrElse("")
      val useragent = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("useragent"))) parese.getJSONObject("rtb_hash").getString("useragent") else ""
      val first_viewed_percent = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxvp"))) parese.getJSONObject("rtb_hash").getString("opxvp") else ""
      val max_viewed_percent = ""
      val countrylong = ipcityconuntry.apply(2)
      val countryshort = ipcityconuntry.apply(1)
      val ipcity = ipcityconuntry.apply(0)
      val tracking_machine = if (StringUtils.isNotBlank(parese.getString("machine"))) parese.getString("machine") else ""
      val ip_filter_flag = 1
      val robot_filter_flag = 1
      val valid_flag = RtbTracking.fraud_validate(parese.getJSONObject("rtb_hash").getString("fraud"))
      val aud_hash = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxaud"))) parese.getJSONObject("rtb_hash").getString("opxaud") else ""
      val crm_hash = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxcrm"))) parese.getJSONObject("rtb_hash").getString("opxcrm") else ""
      val adslot_id = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("slotid"))) parese.getJSONObject("rtb_hash").getString("slotid") else ""
      val canonical = RtbTracking.canonical(parese.getJSONObject("rtb_hash").getString("opxpl")).getOrElse("")
      val domain = RtbTracking.domian(parese.getJSONObject("rtb_hash").getString("opxpl")).getOrElse("")
      val rtb_cookie_id = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("vc"))) parese.getJSONObject("rtb_hash").getString("vc") else ""
      val interest = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxinterest"))) parese.getJSONObject("rtb_hash").getString("opxinterest") else ""
      val age = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxage"))) parese.getJSONObject("rtb_hash").getString("opxage") else ""
      val gender = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxgender"))) parese.getJSONObject("rtb_hash").getString("opxgender") else ""
      val audience = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxaudience"))) parese.getJSONObject("rtb_hash").getString("opxaudience") else ""
      val reason = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxreason"))) parese.getJSONObject("rtb_hash").getString("opxreason") else ""
      val tagid = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("tagId"))) parese.getJSONObject("rtb_hash").getString("tagId") else ""
      //    val fraud=if (StringUtils.isNoneBlank(parese.getJSONObject("rtb_hash").getString("fraud"))) parese.getJSONObject("rtb_hash").getString("fraud") else ""
      val adx_name = if (StringUtils.isNotBlank(parese.getJSONObject("rtb_hash").getString("opxadx"))) parese.getJSONObject("rtb_hash").getString("opxadx") else ""
      val result = uuid + "\t" + bidded_at + "\t" + viewed_at + "\t" + viewed_at_i + "\t" + opxpid + "\t" + opxsid + "\t" + opxbid + "\t" + client_id + "\t" + pretargeting_id + "\t" + searchengine_id + "\t" +
        adgroup_id + "\t" + adtext_id + "\t" + placement_url + "\t" + winning_price + "\t" + cpm + "\t" + ip + "\t" + useragent + "\t" + first_viewed_percent + "\t" + max_viewed_percent + "\t" +
        countrylong + "\t" + countryshort + "\t" + ipcity + "\t" + tracking_machine + "\t" + ip_filter_flag + "\t" + robot_filter_flag + "\t" + valid_flag + "\t" + aud_hash + "\t" + crm_hash + "\t" +
        adslot_id + "\t" + canonical + "\t" + domain + "\t" + rtb_cookie_id + "\t" + interest + "\t" + age + "\t" + gender + "\t" + audience + "\t" + reason + "\t" + tagid + "\t" + adx_name
      if (StringUtils.isBlank(uuid) || StringUtils.isBlank(viewed_at) || StringUtils.isBlank(viewed_at_i) || StringUtils.isBlank(bidded_at) ||
        StringUtils.isBlank(opxpid) || StringUtils.isBlank(opxbid) || StringUtils.isBlank(searchengine_id) || StringUtils.isBlank(adgroup_id) || StringUtils.isBlank(adtext_id)) {
        WriterErrorData.saveErrorData(rtb_save_path_error, result)
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception => SendMail.mail("rtb tracking error occurred", e.toString()); None
    }
  }

  def adgrouptrackinglog(log: String) = {
    //    val logsd = log.split("\\s{2,}").apply(2)

    try {
      val logsd = log.split("\t").apply(2)

      val parese = JSON.parseObject(logsd)

      val ipcityconuntry = AdgroupTracking.ipcity(parese.getJSONObject("query_hash").getString("opxip")).getOrElse(ArrayBuffer("", "", ""))

      val uuid = parese.getString("uuid")
      val click_date = AdgroupTracking.click_date(parese.getJSONObject("query_hash").getString("opxdatetime")).getOrElse("")
      val click_referring_site = if (parese.getJSONObject("query_hash").getString("opxreferer") != null) parese.getJSONObject("query_hash").getString("opxreferer") else ""
      val hash_id: String = AdgroupTracking.hash_id(
        parese.getJSONObject("query_hash").getString("keyword"),
        parese.getJSONObject("query_hash").getString("matchtype"),
        parese.getJSONObject("query_hash").getString("kwid"),
        parese.getJSONObject("query_hash").getString("opxseid").toString,
        parese.getJSONObject("query_hash").getString("opxagid")).getOrElse("")
      val opxpid = parese.getJSONObject("query_hash").getString("opxpid")
      val opxsid = parese.getJSONObject("query_hash").getString("opxsid")
      val searchengine_id = parese.getJSONObject("query_hash").getString("opxseid").toInt
      val c_date = AdgroupTracking.c_date(parese.getJSONObject("query_hash").getString("opxdatetime")).getOrElse("")
      val adtext_id = AdgroupTracking.adtext_id(parese.getJSONObject("query_hash").getString("creative")).getOrElse(-999)
      val adgroup_id = AdgroupTracking.adgroup_id(parese.getJSONObject("query_hash").getString("opxagid")).getOrElse(-999)
      val placement = parese.getJSONObject("query_hash").getString("placement")
      val leading_keyword = AdgroupTracking.leading_keyword(parese.getJSONObject("query_hash").getString("opxreferer"))
      val leading_site = AdgroupTracking.leading_site(parese.getJSONObject("query_hash").getString("opxreferer"))
      val ismobile = AdgroupTracking.ismobile(parese.getJSONObject("query_hash").getString("mobile")).getOrElse("")
      val keyword_key: String = AdgroupTracking.keyword_key(
        parese.getJSONObject("query_hash").getString("keyword"),
        parese.getJSONObject("query_hash").getString("matchtype"),
        parese.getJSONObject("query_hash").getString("kwid"),
        parese.getJSONObject("query_hash").getString("opxseid"),
        parese.getJSONObject("query_hash").getString("opxagid")).getOrElse("")
      val client_id = AdgroupTracking.client_id(parese.getJSONObject("query_hash").getString("opxseid")).getOrElse("")
      val useragent = parese.getJSONObject("query_hash").getString("useragent")
      val ip = parese.getJSONObject("query_hash").getString("opxip")
      //    val browser_ip = parese.getJSONObject("query_hash").getString("opxip")
      val countrylong = ipcityconuntry.apply(2)
      val countryshort = ipcityconuntry.apply(1)
      val ipcity = ipcityconuntry.apply(0)
      val click_machine = if (parese.getString("machine") != null) parese.getString("machine") else ""
      val ip_filter_flag = 1
      val robot_filter_flag = 1
      val valid_flag = AdgroupTracking.fraud_validate(parese.getJSONObject("query_hash").getString("fraud"))
      val bid_request_id = parese.getJSONObject("query_hash").getString("opxbid")
      val aud_hash = if (parese.getJSONObject("query_hash").getString("opxaud") != null) parese.getJSONObject("query_hash").getString("opxaud") else ""
      val crm_hash = if (parese.getJSONObject("query_hash").getString("opxcrm") != null) parese.getJSONObject("query_hash").getString("opxcrm") else ""
      val adslot_id = parese.getJSONObject("query_hash").getString("slotid")
      val canonical = AdgroupTracking.canonical(parese.getJSONObject("query_hash").getString("placement")).getOrElse("")
      val domain = AdgroupTracking.domian(parese.getJSONObject("query_hash").getString("placement")).getOrElse("")
      val tagid = parese.getJSONObject("query_hash").getString("tagId")
      val device = AdgroupTracking.device(parese.getJSONObject("query_hash").getString("devive"),
        parese.getJSONObject("query_hash").getString("opxseid").toString.toInt,
        parese.getJSONObject("query_hash").getString("mobile"),
        parese.getJSONObject("query_hash").getString("useragent")).getOrElse("")

      val ht_score = 101
      val adx_name = parese.getJSONObject("query_hash").getString("opxadx")
      val result = uuid + "\t" + click_date + "\t" + click_referring_site + "\t" + hash_id + "\t" + opxpid + "\t" + opxsid + "\t" + searchengine_id + "\t" + c_date + "\t" + adtext_id + "\t" +
        adgroup_id + "\t" + placement + "\t" + leading_keyword + "\t" + leading_site + "\t" + ismobile + "\t" + keyword_key + "\t" + client_id + "\t" + useragent + "\t" + ip + "\t" +
        "\t" + countrylong + "\t" + countryshort + "\t" + ipcity + "\t" + click_machine + "\t" + ip_filter_flag + "\t" + robot_filter_flag + "\t" + valid_flag + "\t" +
        bid_request_id + "\t" + aud_hash + "\t" + crm_hash + "\t" + adslot_id + "\t" + canonical + "\t" + domain + "\t" + device + "\t" + ht_score + "\t" + tagid + "\t" + adx_name

      if (StringUtils.isBlank(uuid) || StringUtils.isBlank(opxpid) || StringUtils.isBlank(click_date) || StringUtils.isBlank(client_id)) {

        WriterErrorData.saveErrorData(adgroup_sava_path_error, result)
        None
      } else {

        Some(result)
      }
    } catch {
      case e: Exception => SendMail.mail("adgroup tracking error occurred", e.toString()); None
    }
  }

  def eventtrackinglog(log: String) = {

    try {
      val logsplit = log.split("\t").apply(2)
      val parse = JSON.parseObject(logsplit)
      val ipcityconuntry = EventTracking.ipcity(parse.getJSONObject("classic_payload").getString("opxip")).getOrElse(ArrayBuffer("", "", ""))

      val uuid = if (StringUtils.isNotBlank(parse.getString("uuid"))) parse.getString("uuid") else ""
      val event_id = EventTracking.event_id(parse.getJSONObject("classic_payload").getString("opxeventid"))
      val event_date = EventTracking.event_date(parse.getJSONObject("classic_payload").getString("opxdate"), parse.getJSONObject("classic_payload").getString("date")).getOrElse("")
      val referring_site = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxreferringsite"))) parse.getJSONObject("classic_payload").getString("opxreferringsite") else ""
      val event_one = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_one(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_one(event_id)) else ""
      val event_two = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_two(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_two(event_id)) else ""
      val event_three = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_three(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_three(event_id)) else ""
      val event_four = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_four(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_four(event_id)) else ""
      val event_five = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_five(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_five(event_id)) else ""
      val event_six = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString(EventTracking.event_six(event_id)))) parse.getJSONObject("classic_payload").getString(EventTracking.event_six(event_id)) else ""
      val opxpid = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxpid"))) parse.getJSONObject("classic_payload").getString("opxpid") else ""
      val opxsid = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxsid"))) parse.getJSONObject("classic_payload").getString("opxsid") else ""
      val opxid = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxid"))) parse.getJSONObject("classic_payload").getString("opxid") else ""
      val lead_site = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxleadsite"))) parse.getJSONObject("classic_payload").getString("opxleadsite") else ""
      val e_date = EventTracking.event_date(parse.getJSONObject("classic_payload").getString("opxdate"), parse.getJSONObject("classic_payload").getString("date")).getOrElse("")
      val has_hash = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("hascookie"))) parse.getJSONObject("classic_payload").getString("hascookie") else ""
      val leading_site = EventTracking.leading_site(parse.getJSONObject("classic_payload").getString("opxleadsite"))
      val leading_keyword = EventTracking.leading_keyword(parse.getJSONObject("classic_payload").getString("opxleadsite"))
      val useragent = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("useragent"))) parse.getJSONObject("classic_payload").getString("useragent") else ""
      val source_channel = EventTracking.source_channel(parse.getJSONObject("classic_payload").getString("frompaidsearch"), parse.getJSONObject("classic_payload").getString("opxleadsite"))
      val client_id = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("opxclientid"))) parse.getJSONObject("classic_payload").getString("opxclientid") else ""
      val universal_tag_flag = 0
      val ip = EventTracking.ip(parse.getJSONObject("classic_payload").getString("opxip"), parse.getJSONObject("classic_payload").getString("ip"))
      val countrylong = ipcityconuntry.apply(2)
      val countryshort = ipcityconuntry.apply(1)
      val ipcity = ipcityconuntry.apply(0)
      val tracking_machine = if (StringUtils.isNotBlank(parse.getString("machine"))) parse.getString("machine") else ""
      val ip_filter_flag = 1
      val robot_filter_flag = 1
      val domain_filter_flag = 1
      val valid_flag = 1
      val mobile_tag = EventTracking.mobile_tag(ip)
      val ip_8b = EventTracking.ip_8b(ip)
      val ip_16b = EventTracking.ip_16b(ip)
      val ip_24b = EventTracking.ip_8b(ip)
      val browsername = ""
      val browserversion = ""
      val osname = ""
      val osedition = ""
      val device = ""
      val language = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("language"))) parse.getJSONObject("classic_payload").getString("language") else ""
      val accept = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("accept"))) parse.getJSONObject("classic_payload").getString("accept") else ""
      val connection = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("connection"))) parse.getJSONObject("classic_payload").getString("connection") else ""
      val encoding = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("encoding"))) parse.getJSONObject("classic_payload").getString("encoding") else ""
      val hversion = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("hversion"))) parse.getJSONObject("classic_payload").getString("hversion") else ""
      val screen = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("sr"))) parse.getJSONObject("classic_payload").getString("sr") else ""
      val timezone = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("tz"))) parse.getJSONObject("classic_payload").getString("tz") else ""
      val localstorage = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("ls"))) parse.getJSONObject("classic_payload").getString("ls") else ""
      val plugins = ""
      val flash_version = ""
      val flash_index = ""
      val rnum = if (StringUtils.isNotBlank(parse.getJSONObject("classic_payload").getString("rnum"))) parse.getJSONObject("classic_payload").getString("rnum") else ""
      val result = uuid + "\t" + event_id + "\t" + event_date + "\t" + referring_site + "\t" + event_one + "\t" + event_two + "\t" + event_three + "\t" + event_four + "\t" + event_five + "\t" + event_six + "\t" +
        opxpid + "\t" + opxsid + "\t" + opxid + "\t" + lead_site + "\t" + e_date + "\t" + has_hash + "\t" + leading_site + "\t" + leading_keyword + "\t" + useragent + "\t" + source_channel + "\t" +
        client_id + "\t" + universal_tag_flag + "\t" + ip + "\t" + countrylong + "\t" + countryshort + "\t" + ipcity + "\t" + tracking_machine + "\t" + ip_filter_flag + "\t" + robot_filter_flag + "\t" +
        domain_filter_flag + "\t" + valid_flag + "\t" + mobile_tag + "\t" + ip_8b + "\t" + ip_16b + "\t" + ip_24b + "\t" + browsername + "\t" + browserversion + "\t" + osname + "\t" + osedition + "\t" +
        device + "\t" + language + "\t" + accept + "\t" + connection + "\t" + encoding + "\t" + hversion + "\t" + screen + "\t" + timezone + "\t" + localstorage + "\t" + plugins + "\t" + flash_version + "\t" +
        flash_index + "\t" + rnum

      if (StringUtils.isBlank(uuid) || StringUtils.isBlank(event_date) || StringUtils.isBlank(event_one)) {

        WriterErrorData.saveErrorData(event_save_path_error, result)
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception => SendMail.mail("event tracking error occurred", e.toString()); None
    }

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //master

    /*if (args.length < 2) {
      System.err.println("Usage: kafkahost、seconds")
      System.exit(1)
    }
    val conf = new SparkConf().set("spark.driver.maxResultSize", "10g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.streaming.kafka.maxRatePerPartition", "5")
    if (System.getProperty("local") != null) {
      conf.setMaster("local").setAppName("wordSegname")
    }
    val  seconds=args.apply(0).toInt
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(seconds))*/

    //local

    val sc = Models.sc

    val ssc = new StreamingContext(sc, Seconds(200))

    /*val kafkaParm = Map("metadata.broker.list" -> "localhost:9092",
      "auto.offset.reset" -> "smallest", "group.id" -> "group")*/
    val kafkaParm = Map("metadata.broker.list" -> kafka_host,
      "auto.offset.reset" -> "smallest", "group.id" -> group_id)

    val km = new KafkaManager(kafkaParm)
    /*   val numDStreams = 5
    val rtb1= (1 to numDStreams).map{
      _=>km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("rtb_topic"))
    }
    val rtb=ssc.union(rtb1)*/
    val rtb = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("rtb_topic"))
    val adgroup = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("adgroup_topic"))
    val image = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("image_topic"))
    val click = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("click_topic"))
    val event = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParm, Set("event_topic"))
    //更新rtb日志
    rtb.map(_._2).map(x => rtbtrackinglog(x)).filter(_.isDefined).map(_.get).repartition(1).saveAsTextFiles(rtb_save_path)
    rtb.foreachRDD(rdd => km.updateZKOffsets(rdd))

    //更新adgroup日志
    adgroup.map(_._2).map(x => adgrouptrackinglog(x)).filter(_.isDefined).map(_.get).repartition(1).saveAsTextFiles(adgroup_sava_path)
    adgroup.foreachRDD(rdd => km.updateZKOffsets(rdd))

    //更新click日志
    click.map(_._2).map(x => clicktrackinglog(x)).filter(_.isDefined).map(_.get).repartition(1).saveAsTextFiles(click_save_path)
    click.foreachRDD(rdd => km.updateZKOffsets(rdd))

    //更新image的日志

    image.map(_._2).map(x => imagetrackinglog(x)).filter(_.isDefined).map(_.get).repartition(1).saveAsTextFiles(image_save_path)

    image.foreachRDD(rdd => km.updateZKOffsets(rdd))

    //    adgroup.union(click)

    //更新event日志
    event.map(_._2).map(x => eventtrackinglog(x)).filter(_.isDefined).map(_.get).repartition(1).saveAsTextFiles(event_save_path)
    event.foreachRDD(rdd => km.updateZKOffsets(rdd))

    ssc.start() //真正的启动
    ssc.awaitTermination() //阻塞等待

  

  }

}