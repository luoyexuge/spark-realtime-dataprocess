package com.iclick.tracking.util
import java.sql.Connection
import java.sql.DriverManager
object DBManageSer extends  java.io.Serializable {
  val driver: String = "com.mysql.jdbc.Driver";
  val url: String = "jdbc:mysql://10.1.1.28:3306/spark"
  val username: String = "usr_dba";
  val password: String = "4rfv%TGB^YHN"

  def getConnect = {
    try {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, username, password)
      Some(connection)
    } catch {
      case e: Exception => e.printStackTrace(); None
    }
  }
  
  
  def closecon(conn: Connection) {
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

}