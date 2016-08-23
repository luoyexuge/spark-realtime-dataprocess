package com.iclick.tracking.sendmail
import org.apache.commons.io.FileSystemUtils
import org.apache.commons.mail.DefaultAuthenticator
import org.apache.commons.mail.EmailAttachment
import org.apache.commons.mail.EmailException
import org.apache.commons.mail.HtmlEmail
import com.iclick.tracking.util.Loggable
import com.iclick.tracking.util.Config
import com.alibaba.fastjson.JSON

object SendMail extends Loggable {

  val pro = Config.getConfig("config.properties")
  val json = JSON.parseObject(pro.getString("SMTP_OPTIONS"))
  def mail(Subject: String, Msg: String) = {
    val email = new HtmlEmail()
    email.setHostName(json.getString("address"))
    email.setSmtpPort(json.getString("port").toInt)
    email.setAuthenticator(new DefaultAuthenticator(json.getString("user_name"), json.getString("password")))
    email.setSSLOnConnect(true);
    email.setFrom(json.getString("user_name"))
    email.setSubject(Subject);
    email.setMsg(Msg);
    email.addTo("wilson.zhou@i-click.com")
    email.addTo("david.xu@i-click.com")

    email.send()
  }
  def main(args: Array[String]): Unit = {
    mail("dsdasd", "test")
    info("发送成功")
  }
}