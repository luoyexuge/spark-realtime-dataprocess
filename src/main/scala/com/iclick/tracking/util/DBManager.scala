package com.iclick.tracking.util
import java.sql.Connection
import org.apache.commons.dbcp2.BasicDataSource
import javax.sql.DataSource
import com.mysql.jdbc.Driver
import java.sql.DriverManager
import java.io.Serializable
object DBManager  extends Serializable  {

  val datasource: DataSource = {
    val driver: String = "com.mysql.jdbc.Driver";
    val url: String = "jdbc:mysql://10.1.1.28:3306/spark"
    val username: String = "usr_dba";
    val password: String = "4rfv%TGB^YHN"
     val mysqlMaxIdle = 30
    val mysqlMinIdle = 20
    val mysqlMaxTotal = 500
    val  ds=new BasicDataSource
     ds.setDriverClassName(driver)
      ds.setUrl(url)
    ds.setUsername(username)
    ds.setPassword(password)
//    ds.setMaxIdle(mysqlMaxIdle)
//    ds.setInitialSize(mysqlMinIdle)
//    ds.setMaxTotal(mysqlMaxTotal)
//    ds.setMaxWaitMillis(60000)
   
    ds
  }
  
  
  def  getCon:Option[Connection]={
    try{
      val con=datasource.getConnection
      return Some(con)
    }catch {
      case t: Exception => t.printStackTrace
    }
    
    None
  }
  
  def   closecon(conn:Connection){
    try {
      if (conn != null && !conn.isClosed) {
        conn.setAutoCommit(true)
        conn.close
     
      }
    } catch {
      case e: Exception => println("====>MYSQL Connection Close Error !" + e.printStackTrace)
    } finally {
      conn.close()
    }

  }
  def main(args: Array[String]): Unit = {
    val con=getCon.get
    val statement=con.createStatement()
     val rs = statement.executeQuery("select * from wordsegment limit 10 ")
      while (rs.next) {
        val host = rs.getInt("age")
        val user = rs.getString("name")
        println("host = %s, user = %s".format(host, user))
      }
    
    statement.execute("""insert into wordsegment(name,id,age) values ("lklk","1",5)""")
    closecon(con)
  }

}