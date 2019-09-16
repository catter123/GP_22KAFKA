package DEF
import java.lang
import java.sql.Statement

import Utils.{JedisConnectionPool, MysqlConnection}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable
/**
  * @author catter
  * @date 2019/8/27 18:57
  */
object KafkaRedisOffset {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[3]")
      //设置Kafka拉取数据的频率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      //设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //Stream定义
    val ssc = new StreamingContext(conf,Seconds(3))
    //todo 配置kafka参数
    //组名
    val GroupId="group1798889"
    //topic
    val topic="fasw"
    //指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList="hadoop01:9092,hadoop02:9092,hadoop03:9092"
    //编写Kafka的配置参数
    val kafkas: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      //kafka的Key和value的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GroupId,
      //消费方式
      "auto.offset.reset" -> "earliest",
      //不需要 程序自动提交
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //todo 创建topic集合
    val topics = Set(topic)

    //todo 获取数据
    //第一步，获取offest
    //第二步，通过offest获取卡夫卡数据
    //第三步，提交更新offest

    //第一步，获取offest
    val fromOffest: Map[TopicPartition, Long] = JedisOffset(GroupId)
    // 判断是否有数据‘
    val stream: InputDStream[ConsumerRecord[String,String]] = if (fromOffest.size == 0) {
      println("11111111111111111111111111111111111")
      KafkaUtils.createDirectStream(ssc,
        //本地策略
        //将数据均匀分配到各个exector上
        LocationStrategies.PreferConsistent,
        //消费者策略
        //动态增加分区
        ConsumerStrategies.Subscribe[String, String](topics, kafkas)
      )
    } else {
      //不是第一次消费数据
      println("222222222222222222222222222222222")
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffest.keys, kafkas, fromOffest)
      )
    }


    val ddd = ssc.sparkContext.textFile("C:\\Users\\catter\\Desktop\\ip.txt")

    val streamw: RDD[(Long, Long, String)] = ddd.map(x => {
      val arrays = x.split("\\|")
      val starts = arrays(2).toLong
      val endd = arrays(3).toLong
      val province = arrays(6)
      (starts, endd, province)
    })

    println("steream")
    val brocat: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(streamw.collect())
    stream.foreachRDD({
      rdd=>

      println("zhixing")

        val offestRangse: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val jedis: Jedis = JedisConnectionPool.getConn()
      println(offestRangse.length+"--------------")

          val gg: RDD[String] = rdd.map(_.value())
          val productAll: RDD[(String, Long, String, String, Long)] =gg.map(
            j=>{
              val strarray = j.split(" ")
              val date = strarray(0)
              val ip = CountMoney.ip2Long(strarray(1))
              val topy = strarray(2)
              val product = strarray(3)
              val money = strarray(4).toLong
              (date, ip, topy, product, money)
            })

            //计算总金额
            CountMoney.countAllMoney(productAll.map(_._5))
            //计算类别成交量
            CountMoney.countTopynum(productAll.map(_._4))

            productAll.foreach(x=>{
              val jedis: Jedis = JedisConnectionPool.getConn()
              val ip = x._2
              val money = x._5
              val proo = CountMoney.Binary(ip,brocat.value)
              jedis.hincrBy("城市",proo,money)
              jedis.close()
            })

          //业务处理

        //todo 变量参数
////        rdd.foreachPartition(
////          a=>{
//
////            val statement: Statement = MysqlConnection.getStatement()
////            val jedis = JedisConnectionPool.getConn()
//
////            a.foreach(
////              x=>{
////
////                )
////
////
////              val jsondata: JSONObject = JSON.parseObject(x.value())
////              //时间
////              val logtime = jsondata.getString("logOutTime")
////              val timea8 = logtime.substring(0,8)
////              val timea = logtime.substring(0,10)
////              val timemin: String = logtime.substring(0,12)
////              //价格
////              val chargefee = jsondata.getString("chargefee").toLong
////              //省份
////              val provinceCode: String = jsondata.getString("provinceCode")
////
////              val cityName = jedis.hget("citydir",provinceCode)
////
////              //todo test
////              //test1.每分钟，每小时传输和总数据
////              CountMoneyMain.gettest1(jedis,jsondata,timea8,timea,chargefee,statement)
////              //test2 统计每小时各个省份的充值失败数据量
////              CountMoneyMain.getFailpay(jedis,jsondata,cityName,timea8,statement)
////              //test3 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数
////              CountMoneyMain.getProTop10(jsondata,cityName,timea8,statement)
//
//
//
//            })
//
//
//            jedis.close()
////            statement.close()
//
//        })


      println("jdiszhubei")
        //更新偏移量
        for (or<-offestRangse){
          jedis.hset(GroupId,or.topic+"-"+or.partition,or.untilOffset.toString)
          println("jideszhixingzhong")
        }

        jedis.close()
      rdd.map(_.value())
    } )


    ssc.start()
    ssc.awaitTermination()
    
  }
}
