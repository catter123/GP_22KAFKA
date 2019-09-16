package DEF

import Utils.JedisConnectionPool
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author catter
  * @date 2019/8/28 17:06
  */
object ProidtoProvince {

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    var cityid=""
    var cityName=""
    val strs: RDD[String] = sc.textFile("E:\\搜狗\\项目day07\\充值平台实时统计分析\\city.txt")
    strs.foreachPartition(
      t=>{
        val jedis = JedisConnectionPool.getConn()
        t.foreach(k=>{
          val str = k.split(" ")

          cityid=str(0)
          cityName=str(1)
          jedis.hset("citydir",cityid,cityName)
        })
        jedis.close()
      })


  }


}
