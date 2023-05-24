package com.redislabs.provider.redis.df.standalone

import com.redislabs.provider.redis.df.TextDataframeSuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.apache.spark.sql.redis.RedisSourceRelation.dataKey

import java.nio.charset.StandardCharsets.UTF_8

/**
  * @author The Viet Nguyen
  */
class TextDataframeStandaloneSuite extends TextDataframeSuite with RedisStandaloneEnv {

  override def saveMap(tableName: String, key: String, value: Map[String, String]): Unit = {
    val host = redisConfig.initialHost
    withConnection(host.connect()) { conn =>
      conn.set(dataKey(tableName, key).getBytes(UTF_8), serialize(value))
    }
  }
}
