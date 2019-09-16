package DEF

import Utils.JedisConnectionPool
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author catter
  * @date 2019/8/31 15:50
  */
object LocationWithid {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\catter\\Desktop\\ip.txt")
    lines.foreachPartition(x=>{
      val jedis = JedisConnectionPool.getConn()
      x.foreach(line => {
        val arrays = line.split("\\|")
        val start = arrays(2)
        val endd = arrays(3)
        val province = arrays(6)
        val key=start+":"+endd
        jedis.set("city:"+key,province)
        })
        jedis.close()
      })



  }



}
