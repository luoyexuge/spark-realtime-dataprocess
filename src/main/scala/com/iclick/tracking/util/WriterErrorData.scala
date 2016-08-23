package com.iclick.tracking.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.text.SimpleDateFormat
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.BufferedWriter
import java.io.OutputStreamWriter
object WriterErrorData {
  val prop = Config.getConfig("config.properties")
  val HdfsConfigPath = prop.getString("hadoop.hdfs.config.path")
  val conf = new Configuration()
  conf.addResource(new Path(HdfsConfigPath + "core-site.xml"))
  conf.addResource(new Path(HdfsConfigPath + "hdfs-site.xml"))

  def saveErrorData(path: String, str: String) = {
    val time = System.currentTimeMillis()
    val save_path = path + time.toString()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(save_path), true)
    val byt = str.getBytes()
    val len = byt.length
    if (len != -1) {
      out.write(byt)
      out.close()
    }

  }

  def main(args: Array[String]): Unit = {

    val a=1
    val b=2
    val c=3
    if(a<1||b<2||c>=3){
      println("成立")
    }else{
      println("不成立")
    }

  }

}