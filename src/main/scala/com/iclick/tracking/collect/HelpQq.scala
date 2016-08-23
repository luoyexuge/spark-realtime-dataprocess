package com.iclick.tracking.collect
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
object HelpQq {

  def main(args: Array[String]): Unit = {

    /*  20160713000245170,冀TDC582,HD040-4,20160713001320,INIFile.Passing,8666,2,
    北向南,王串场一号路与育才路交口,HD040,120203000000010407,王串场一号路与育才路交口北向南,冀TDC582,小型汽车,蓝色,小汽车,
    2016-07-13 00:02:45 170,2016-07-13 00:02:45 170,卡口,,J,4,20160713000245170,2,0,0,0,2016-07-13_00-02-45-218_1.jpg,
    ftp://itsterminal:itsterminal@120.102.98.190:21/sata/0/0/specspace/TDCapture/2016/07/13/00/05/2016-07-13_00-02-45-218_1.jpg,
*/

    val conf = new SparkConf().setAppName("SMS Mess classification(Ham or Spm)").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df =sc.parallelize(Seq((1,2),(10,20))).toDF("A","B")
    val df1=df.withColumn("C", col("B")*100)
    df1.show()

  }

}