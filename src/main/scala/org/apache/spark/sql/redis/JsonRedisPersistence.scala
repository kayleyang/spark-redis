package org.apache.spark.sql.redis

import com.redislabs.provider.redis.util.ParseUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Pipeline

import scala.util.parsing.json.{JSON, JSONObject}

/**
  * @author The Viet Nguyen
  */
class JsonRedisPersistence extends RedisPersistence[String] {

  override def save(pipeline: Pipeline, key: String, value: String, ttl: Int): Unit = {
    if (ttl > 0) {
      pipeline.setex(key, ttl.toLong, value)
    } else {
      pipeline.set(key, value)
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit =
    pipeline.get(key)

  override def encodeRow(keyName: String, value: Row): String = {
    val fields = value.schema.fields.map(_.name)
    val kvMap = value.getValuesMap[Any](fields)
    kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .map { case (k, v) =>
        k -> String.valueOf(v)
      }
    JSONObject(kvMap).toString()
  }

  override def decodeRow(keyMap: (String, String), value: String, schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    JSON.globalNumberParser = {input: String => input}
    var results = JSON.parseFull(value).getOrElse(0).asInstanceOf[Map[String, String]]
    results += keyMap
    val fieldsValue = ParseUtils.parseFields(results.filterKeys(requiredColumns.toSet), schema)
    new GenericRowWithSchema(fieldsValue, schema)
  }
}
