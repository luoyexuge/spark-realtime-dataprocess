package com.iclick.tracking.util
import java.util.Properties
import java.io.InputStream
import java.io.IOException
import scala.collection.mutable.Map
import java.io.FileInputStream

trait RichProperties {
  def getProperty(name: String): String
  
  def getInt(name: String, d: Int): Int = {
    val v = getProperty(name)
    if (v == null) return d else v.trim().toInt
  }
  
  def getDouble(name: String, d: Double): Double = {
    val v = getProperty(name)
    if (v == null) return d else v.trim().toDouble
  }
  
  def getBoolean(name: String, d: Boolean): Boolean = {
    val v = getProperty(name)
    if (v == null) d else v.trim().toBoolean
  }
  
  def getString(name: String): String = {
    val v = getProperty(name)
    if (v == null) null else v.trim()
  }
  
  def getList(name: String): List[String] = {
    val v = getProperty(name)
    if (v == null) List.empty[String] else v.split(",").toList map {x => x.trim()}
  }
  
  def getIntList(name: String): List[Int] = {
    val v = getProperty(name)
    if(v == null) List.empty[Int] else v.split(",").toList map {x => x.trim().toInt}
  }
}

object Config extends Loggable {
  def getConfig(filename: String): Properties with RichProperties = {
    val prop = new Properties with RichProperties
    prop.putAll(getProperties(filename))
    prop.putAll(getProperties(filename + ".local"))
    prop
  }
  
  def loadProps(filePath: String): Properties with RichProperties = {
    val props = new Properties
    val fis = new FileInputStream(filePath)
    props.load(fis)
    fis.close()
    new Properties(props) with RichProperties
  }
  
  private def getProperties(filename: String): Properties = {
		val prop = new Properties
		var is: InputStream = null;
		try {
			is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)
			if (is != null) prop.load(is)
			return prop
		} catch { 
		  case e: Exception =>
			warn("Error while loading file: " + filename, e)
			return prop
		} finally {
			try {
				if (is != null) is.close()
			} catch { case e: IOException => {}}
		}
	}
  
  def main(args: Array[String]): Unit = {
    val pro=Config.getConfig("config.properties")
    info(pro.getString("mysql.host"))
    info(pro.getString("mysql.driver"))
 
  }
}