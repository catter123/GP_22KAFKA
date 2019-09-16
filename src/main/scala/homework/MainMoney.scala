package homework

import java.lang
import java.sql.Statement

import DEF.{CountMoneyMain, JedisOffset}
import Utils.{JedisConnectionPool, MysqlConnection}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable

/**
  * @author catter
  * @date 2019/8/29 17:33
  */
object MainMoney {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(
      this.getClass.getName).setMaster("local[3]")
      //设置Kafka拉取数据的频率
      .set("spark.streaming.kafka.maxRatePerPartition","10")
      //设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //Stream定义
    val ssc = new StreamingContext(conf,Seconds(3))

    //todo 配置kafka参数
    //组名
    val GroupId="group0391"
    //topic
    val topic="ttccc"
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
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffest.keys, kafkas, fromOffest)
      )
    }





    stream.foreachRDD({
      rdd=>
        val offestRangse: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //业务处理

        //todo 变量参数
        rdd.map(_.value()).foreach(
          str=>  {
           val jedis = JedisConnectionPool.getConn()
                val json: JSONObject = JSON.parseObject(str)
                //日期
                val strs: String = json.getString("date")
                val times = strs.substring(0,10)
                val mon = strs.substring(0,7)
                //金额
                val money = json.getString("money").toLong
                //手机号
                val phone = json.getString("phoneNum")


                //需求
                jedis.incrBy("test1"+times,money)
                jedis.hincrBy(phone+":"+mon,"money",money)
                jedis.hincrBy(phone+":"+mon,"num",1)
                val phoneMoney = jedis.hget(phone+":"+mon,"money").toDouble
                val num = jedis.hget(phone+":"+mon,"num").toDouble
                jedis.hset("avg",phone,(phoneMoney/num).toString)

               })




        val jedis = JedisConnectionPool.getConn()

        //更新偏移量
        for (or<-offestRangse){
          jedis.hset(GroupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
