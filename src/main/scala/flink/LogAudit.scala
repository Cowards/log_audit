package flink

import bean.Kafka_Constant
import source.FlinkKafkaSource

object LogAudit {
  def main(args: Array[String]): Unit = {
    val zookeeperhost = Kafka_Constant.ZOOKEEPER_HOST
    val kafkabroker = Kafka_Constant.KAFKA_BROKER
    val transactiongroup = Kafka_Constant.TRANSACTION_GROUP
    //将需要消费的多个topic写入list
    val kafkaList: List[String]= List("test-demo", "kafka-demo", "flink-demo")
    val topicKafka = "kafka-flink-log-test"
    FlinkKafkaSource.getKafkaSource(zookeeperhost,kafkabroker,transactiongroup,topicKafka)

  }
}
