package DEF

/**
  * @author catter
  * @date 2019/8/31 17:12
  */
package huangYuMing

package Test

import Utils.JedisConnectionPool
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object endds {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = JedisConnectionPool.getConn()
    jedis.flushAll()
    jedis.close()

    // 0、冗长的准备工作
    val groupId = "group"
    val topic = "testa55333"

    val conf = new SparkConf().setAppName("offset").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(10))

    val Ip: RDD[String] = ssc.sparkContext.textFile("C:\\Users\\catter\\Desktop\\ip.txt")
    val IPArr = Ip.map(x => {
      val arr = x.split("\\|")
      //      val jedis: Jedis = JedisConnectionPool.getConnection()
      //      jedis.incrBy("Exam1", arr(4).toInt)
      //      jedis.hincrBy("Exam2", arr(2), 1)
      //      val IpArr: Array[(Long, Long, String)] = IpArrb.value
      //      val Ip = ip2Long(arr(1))
      //      val pro = binarySearch(IpArr,Ip)
      //      jedis.hincrBy("Exam3", pro, arr(4).toInt)
      //      jedis.close()
      (arr(2).toLong , arr(3).toLong , arr(6))
    })
    val IpArrb: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(IPArr.collect)

    val kafkas = Map[String,Object](
      "bootstrap.servers"->"hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      "auto.offset.reset"-> "earliest",
      "enable.auto.commit"-> (false: java.lang.Boolean)
    )
    val topics = Array(topic)

    // 1、提取对应分区的Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId+":"+topic)

    // 2、根据Offset提取数据
    val logs:InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }

    // 3、储存对应分区的Offset
    logs.foreachRDD(rdd =>{
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val offestRange = ranges
      val jedis = JedisConnectionPool.getConn()
      for (or<-offestRange){
        jedis.hset(groupId+":"+topic, or.topic+"-"+or.partition, or.untilOffset.toString)
      }
      jedis.close()
    })
    val ds: DStream[(String, Int)] = logs.map(_.value()).map(line => {
      val arr: Array[String] = line.split(" ")
      val jedis: Jedis = JedisConnectionPool.getConn()
      jedis.incrBy("Exam1", arr(4).toInt)
      jedis.hincrBy("Exam2", arr(2), 1)
      jedis.close()
      (arr(1), arr(4).toInt)
    })
    ds.print()

    // 4、
    val res: DStream[Unit] = ds.map(x => {
      val jedis: Jedis = JedisConnectionPool.getConn()
      val IpArr: Array[(Long, Long, String)] = IpArrb.value
      val s = x._1.split("[.]")
      var ipNum = 0L
      for (i <- 0 until s.length) {
        ipNum = s(i).toLong | ipNum << 8L
      }
      val Ip = ipNum
      val pro = binarySearch(IpArr, Ip)
      jedis.hincrBy("Exam3", pro, x._2)
      jedis.close()
    })
    res.print()


    // 收尾
    ssc.start()
    ssc.awaitTermination()
  }

  def binarySearch(IpArr:Array[(Long, Long, String)],Ip:Long):String = {
    var le = 0
    var ri = IpArr.length
    while(le<ri-1){
      var mi = ((le + ri) / 2)
      if(Ip>IpArr(mi)._1&&Ip<IpArr(mi)._2) return IpArr(mi)._3
      else if(Ip<IpArr(mi)._1){
        ri = mi
      }
      else if(Ip>IpArr(mi)._2){
        le = mi
      }
      println(le)
      println(ri)
    }
    "该Ip地址无效"
  }

  def ip2Long(ip:String):Long ={
    val s = ip.split("[.]")
    var ipNum =0L
    for(i<-0 until s.length){
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }

}
