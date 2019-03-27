package com.wedata.stream.app

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, scala}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import _root_.java.time.ZoneId

object LogAuditFlowBase {

  private val ZOOKEEPER_HOST = "cdh4:2181,cdh5:2181,cdh6:2181"
  private val KAFKA_BROKER = "cdh4:9092,cdh5:9092,cdh6:9092"
  private val TRANSACTION_GROUP = "audit_sql_group"
  //baseTopic
  private val topicKafka_base = "log_audit_base"
  //hdfs文件存储路径
  private val hdfsPathBase = "hdfs:///tmp/table_temp/log_audit_kafka_flink_base"
  //kafkaSource名
  private val kafka_source_base = "kafka_source_base"
  //sink名
  private val sink_2_hdfs_base = "sink_2_hdfs_base"
  //sourceMethod
  def main(args: Array[String]): Unit = {

    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    //kafka consumer
    val kafkaProps = new Properties()
    //连接zookeeper
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    //连接kafkaBroker
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topicKafka_base, new SimpleStringSchema(), kafkaProps)
    val transac = env.addSource(kafkaConsumer).setParallelism(2).name(kafka_source_base)

    val sink2hdfs = new BucketingSink[String](hdfsPathBase)
    sink2hdfs.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
    sink2hdfs.setBatchSize(128 * 1024 * 1024) // this is 128 MB, block file
    sink2hdfs.setBatchRolloverInterval(20 * 60) // this is 5s
    transac.addSink(sink2hdfs).setParallelism(3).name(sink_2_hdfs_base)
    env.execute()

  }

}