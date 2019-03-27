package com.wedata.stream.app

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import _root_.java.time.ZoneId


object LogAuditFLow {
  //sourceMethod
  def startJob(zookeeper: String, bootstrap: String, groupid: String, topic: String): Unit = {
    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    //kafka consumer
    val kafkaProps = new Properties()
    //连接zookeeper
    kafkaProps.setProperty("zookeeper.connect", zookeeper)
    //连接kafkaBroker
    kafkaProps.setProperty("bootstrap.servers", bootstrap)
    kafkaProps.setProperty("group.id", groupid)
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema(), kafkaProps)
    val transac = env.addSource(kafkaConsumer).setParallelism(2).name("Kafka Source")

    val hdfsPath = "hdfs:///tmp/table_temp/log_audit_kafka_flink"

    //先转换成hdfs的文件再和hive关联
    val sink2hdfs = new BucketingSink[String](hdfsPath)
    sink2hdfs.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
    sink2hdfs.setBatchSize(128 * 1024 * 1024) // this is 128 MB, block file
    sink2hdfs.setBatchRolloverInterval(20 * 60) // this is 5s
    transac.addSink(sink2hdfs).setParallelism(3).name("Sink HDFS")

    //-----



    //-----
    env.execute()
  }

  private val ZOOKEEPER_HOST = "cdh1:2181,cdh2:2181,cdh3:2181"
  private val KAFKA_BROKER = "cdh4:9092,cdh5:9092,cdh6:9092"
  private val TRANSACTION_GROUP = "audit_sql_group"

  def main(args: Array[String]): Unit = {
    val topicKafka = "audit_sql"
    startJob(ZOOKEEPER_HOST,KAFKA_BROKER,TRANSACTION_GROUP,topicKafka)
  }
}