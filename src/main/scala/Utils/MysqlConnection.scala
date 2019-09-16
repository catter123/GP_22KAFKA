package Utils

import java.sql.{DriverManager, Statement}

/**
  * @author catter
  * @date 2019/8/28 21:16
  */
object MysqlConnection {
  def getStatement():Statement={
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8","root","catter")
    val statement = connection.createStatement()
    statement
  }

}
