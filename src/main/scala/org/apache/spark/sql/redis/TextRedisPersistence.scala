package org.apache.spark.sql.redis

import com.redislabs.provider.redis.util.ParseUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import redis.clients.jedis.Pipeline

/**
  * @author The Viet Nguyen
  */
class TextRedisPersistence extends RedisPersistence[String] {

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
    kvMap.values.mkString(",")
  }

  override def decodeRow(keyMap: (String, String), value: String, schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    val newSchema = StructType(Array(StructField("value", StringType)))
    val results = Array(("value", value)) :+ keyMap
    val fieldsValue = ParseUtils.parseFields(results.toMap, newSchema)
    new GenericRowWithSchema(fieldsValue, newSchema)
  }
}
