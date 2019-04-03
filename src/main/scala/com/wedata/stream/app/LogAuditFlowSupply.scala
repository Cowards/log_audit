package com.wedata.stream.app

import java.util.{Date, Properties}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, scala}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BasePathBucketer, BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import _root_.java.time.ZoneId
import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.connectors.fs.{NonRollingBucketer, RollingSink, StringWriter}

object LogAuditFlowSupply {
  private val ZOOKEEPER_HOST = "cdh4:2181,cdh5:2181,cdh6:2181"
  private val KAFKA_BROKER = "cdh4:9092,cdh5:9092,cdh6:9092"
  private val TRANSACTION_GROUP = "audit_sql_group_supply_4"
  //supplyTopic
  private val topicKafka_supply = "log_audit_supply"
  //kafkaSource名
  private val kafka_source_supply = "kafka_source_supply"
  //sink名
  private val sink_2_hdfs_supply = "sink_2_hdfs_supply"
  //sourceMethod

  def main(args: Array[String]): Unit = {
    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
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
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topicKafka_supply, new SimpleStringSchema(), kafkaProps)
    val transac = env.addSource(kafkaConsumer).setParallelism(2).name(kafka_source_supply)

    val dateTimeBucketer = new Date()
    val fdf = FastDateFormat.getInstance("yyyy-MM-dd")
    val dateTimeBucketerStr = fdf.format(dateTimeBucketer)
    val hdfsPathSupply =  s"hdfs:///tmp/table_temp/log_audit_kafka_flink_supply/dt=${dateTimeBucketerStr}"
    val sink2hdfs = new BucketingSink[String](hdfsPathSupply)
    sink2hdfs.setBucketer(new BasePathBucketer[String]())
    sink2hdfs.setBatchSize(128 * 1024 * 1024) // this is 128 MB, block file
    sink2hdfs.setWriter(new StringWriter())
    sink2hdfs.setInactiveBucketCheckInterval(1L)
    sink2hdfs.setInactiveBucketThreshold(1L)
    transac.addSink(sink2hdfs).setParallelism(3).name(sink_2_hdfs_supply)
    env.execute()
  }
}