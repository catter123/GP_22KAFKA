package DEF

import java.util

import Utils.JedisConnectionPool
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

/**
  * @author catter
  * @date 2019/8/27 17:49
  */
object JedisOffset {

  def apply(groupId:String):Map[TopicPartition,Long]={
    //创建最后返回类型
    var formdbOffest = Map[TopicPartition,Long]()
    //创建jedis连接
    val jedis: Jedis = JedisConnectionPool.getConn()
    //查询jedis所有的topic，partition
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupId)
    //需要执行隐式转换操作
    import scala.collection.JavaConversions._
    //map转换为list循环处理
    val topicPartitionOffsetList: List[(String, String)] = topicPartitionOffset.toList
    //循环处理数据
    for (topicPL <-topicPartitionOffsetList){
      val str: Array[String] = topicPL._1.split("[-]")
      formdbOffest += (new TopicPartition(str(0),str(1).toInt)->topicPL._2.toLong)
    }
    formdbOffest
  }


}
