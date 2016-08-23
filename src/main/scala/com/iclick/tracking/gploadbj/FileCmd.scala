package com.iclick.tracking.gploadbj
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.collection.mutable.ListBuffer
import com.iclick.tracking.util.Loggable
import com.iclick.tracking.util.Config
import com.iclick.tracking.sendmail.SendMail

object FileCmd extends Loggable {
  val prop = Config.getConfig("config.properties")
  val GPHDFSLOC=prop.getString("GPHDFSLOC")
  val GPHOST=prop.getString("GPHOST")
  val GPUSER=prop.getString("GPUSER")
  val GPDB=prop.getString("GPDB")
  val run = Runtime.getRuntime()

  //  "hadoop fs -ls /tmp/wilson/collectadclicklog/ds=*/_SUCCESS
  def runsystem(cmd: String,contains_str:String) = {
    val list = ListBuffer[String]()
    val p = run.exec(cmd)
    p.wait()
    val in = new BufferedInputStream(p.getInputStream())
    val inbr = new BufferedReader(new InputStreamReader(in))
    var str = inbr.readLine()
    while (str != null) {
      if (str.contains(contains_str)) {
        list.append(str)
      }
      str = inbr.readLine()
    }
    inbr.close()
    in.close()
    list.flatMap {
      x => x.split("(\\s){1,}").toList.filter { x => x.contains(contains_str) }
    }.map(x => x.replace("_SUCCESS", "")).sortWith(_<_).toSet

  }

  def runsql(sql: String) = {
    val p = run.exec(sql)
    p.wait()

  }

  def get_tohandledata(cmd1: String, cmd2: String,contains_str:String) = {
    val parsed_set = runsystem(cmd1,contains_str)
    val gpload_set = runsystem(cmd2,contains_str)
    
    if (parsed_set.size > gpload_set.size) {
      parsed_set.diff(gpload_set)
    } else {
      gpload_set.diff(parsed_set)
    }

  }

  

  def main(args: Array[String]): Unit = {
     val list=(for(i<-1 to 10) yield i).sum
    println(list)
     
     println()

  }

}