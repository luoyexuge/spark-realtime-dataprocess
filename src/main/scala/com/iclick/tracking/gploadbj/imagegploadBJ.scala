package com.iclick.tracking.gploadbj
import com.iclick.tracking.sendmail.SendMail
import com.iclick.tracking.util.Loggable
object imagegploadBJ extends  Loggable{
    def togp(cmd1: String, cmd2: String,contains_str:String) = {
    val set = FileCmd.get_tohandledata(cmd1, cmd2,contains_str)
    set.foreach {
      x =>
        
       val batch_id=x.split("-").last
       val hdfsloc=FileCmd.GPHDFSLOC+x+"part-00000"
       try{
       val cmd=s"""psql -h "${FileCmd.GPHOST}" -U "${FileCmd.GPUSER}"  "${FileCmd.GPDB}" -f image_hd_to_gp.sql -v batchid='${batch_id}' -v hdfsloc='${hdfsloc}'"""
       val p=FileCmd.run.exec(cmd)
       p.wait()
       val p1=FileCmd.run.exec(s"hadoop fs -touchz ${x}_GPLOAD ")
       p1.wait()
       }catch{
         case e:Exception=>SendMail.mail("image toGP error occurred",e.toString())
       }
    }

  }
    def main(args: Array[String]): Unit = {
     
      val cmd1="hadoop fs -ls /staging/tracking/parsed/image/image-*/_SUCCESS"   
      val cmd2="hadoop fs -ls /staging/tracking/parsed/image/image-*/_GPLOAD"
      togp(cmd1, cmd2, "/parsed/image")
      
    }
  
  
}