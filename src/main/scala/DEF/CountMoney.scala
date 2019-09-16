package DEF

import Utils.JedisConnectionPool
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * @author catter
  * @date 2019/8/31 15:35
  */
object CountMoney {

  def  countAllMoney(money: RDD[Long]): Unit ={

    money.foreach(m=>{
      val jedis: Jedis = JedisConnectionPool.getConn()
      jedis.incrBy("总额",m)
      jedis.close()
    })

  }


  def  countTopynum(topy: RDD[String]): Unit ={

    topy.foreach(t=>{
      val jedis: Jedis = JedisConnectionPool.getConn()
      jedis.incrBy(t,1)
      jedis.close()
    })

  }

  def ip2Long(ip:String):Long ={
    val s = ip.split("[.]")
    var ipNum =0L
    for(i<-0 until s.length){
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }



  def Binary(ip:Long,arrayip:Array[(Long, Long, String)]): String = {
    var left = 0
    var right = arrayip.length
    var pro=""
    var might =(left+right)/2
      while (left<right-1){
     might = (left+right)/2
      if(ip>arrayip(might)._2){
        left=might
      }else if(ip<arrayip(might)._1){
        right=might
      }else if(ip>=arrayip(might)._1&&ip<=arrayip(might)._2){
        pro=arrayip(might)._3
      }else{
        println("nucunzai ")
      }
    }

pro
  }

}
