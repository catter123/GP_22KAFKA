package Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author catter
  * @date 2019/8/27 17:38
  */
object JedisConnectionPool {
  //获取jedis配置
  private val config = new JedisPoolConfig
  //设置最大连接数和连接池最大容量
  config.setMaxIdle(10)
  config.setMaxTotal(30)
  //连接虚拟机jedis并给与配置
  private val pool = new JedisPool(config,"hadoop01",6379)
  //获取连接
  def getConn():Jedis={
    pool.getResource
  }


}
