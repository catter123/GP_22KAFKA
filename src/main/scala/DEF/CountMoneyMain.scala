package DEF

/**
  * @author catter
  * @date 2019/8/28 15:36
  */

import java.lang
import java.sql.{ResultSet, Statement}
import java.util.Properties

import Utils.{JedisConnectionPool, MysqlConnection}
import com.alibaba.fastjson.{JSON, JSONObject}
import redis.clients.jedis.Jedis


/**
  * test1
  */
object CountMoneyMain {
  def gettest1(jedis: Jedis,jsondata:JSONObject,timemin: String,timea:String,chargefee:Long,statement:Statement): Unit = {
    val bussinessRst = jsondata.getString("bussinessRst")


    //判断是否充值成功
    if (jsondata.getString("serviceName").equals("sendRechargeReq")) {
      //总消费数量
      jedis.hincrBy("orderwithMin:" + timemin, "orderSum", 1)
      jedis.incrBy("orderwithAll:orderSum", 1)
      if (bussinessRst.equals("0000")) {

        // todo 统计总金额每小时传输到mysql

        val str = s"select  * from moneycount where ttt='${timea}'"
        val resultSet = statement.executeQuery(str)
        if (!resultSet.next()) {
          statement.execute(s"insert into moneycount(ttt,money,num) values('${timea}',${chargefee},1)")
        } else {
          statement.execute(s"update moneycount set money=money+${chargefee},num=num+1 where ttt='${timea}'")
        }

        jedis.hincrBy("orderwithMin:" + timemin, "Vorder", 1)
        jedis.hincrBy("orderwithMin:" + timemin, "money", chargefee)
        jedis.incrBy("orderwithAll:Vorder", 1)
        jedis.incrBy("orderwithAll:money", chargefee)

      } else {
        jedis.hincrBy("orderwithMin:" + timemin, "Forder", 1)
        jedis.incrBy("orderwithAll:Forder", 1)
      }

    }


  }

  /**
    * test2
    */
  def getFailpay(jedis: Jedis,jsondata:JSONObject,cityName: String,timea:String,statement:Statement): Unit ={
    val bussinessRst = jsondata.getString("bussinessRst")
    if(jsondata.getString("serviceName").equals("sendRechargeReq")){
      if(!bussinessRst.equals("0000")){


        val str=s"select *  from cityfail where cityName='${cityName}'"
        val reset: ResultSet = statement.executeQuery(str)
        if(!reset.next()){
          statement.execute(s"insert into cityfail(cityName,cityNum,time) values('${cityName}',1,'${timea}')")
        }else{
          statement.execute(s"update cityfail set cityNum=cityNum+1 where cityName='${cityName}' and time='${timea}'")
        }

      }
    }
  }
  /**
    * test3
    */

  def getProTop10(jsondata:JSONObject,cityName:String,timea:String,statement:Statement): Unit ={
    var success=0
    val bussinessRst: String = jsondata.getString("bussinessRst")
    if(jsondata.getString("serviceName").equals("sendRechargeReq")){
      if(bussinessRst.equals("0000")){
        success=1
      }else{
        success=0
      }
    }

    val str=s"select * from cityandnum where cityName='${cityName}' and time='${timea}'"
    val reset: ResultSet = statement.executeQuery(str)
    if(!reset.next()){
      statement.execute(s"insert into cityandnum(cityName,cityNum,time,successnum) values('${cityName}',1,'${timea}',1)")
    }else{
      statement.execute(s"update cityandnum set cityNum=cityNum+1,successnum=successnum+${success} where cityName='${cityName}' and time='${timea}'")
    }
    statement.execute("truncate table citytop")
    statement.execute("insert into citytop (SELECT cityName,successnum/cityNum as cityNum,time from  cityandnum ORDER BY cityNum desc limit 10) ")


  }
}


