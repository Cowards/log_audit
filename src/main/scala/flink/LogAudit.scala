package flink

import source.FlinkKafkaSource

object LogAudit {
  private val ZOOKEEPER_HOST = "cdh4:2181,cdh5:2181,cdh6:2181"
  private val KAFKA_BROKER = "cdh4:9092,cdh5:9092,cdh6:9092"
  private val TRANSACTION_GROUP = "audit_sql_group"

  def main(args: Array[String]): Unit = {
    val topicKafka = "audit_sql"
    FlinkKafkaSource.getKafkaSource(ZOOKEEPER_HOST,KAFKA_BROKER,TRANSACTION_GROUP,topicKafka)
  }
}
