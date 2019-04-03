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
  private val ZOOKEEPER_HOST = "cdh1:2181,cdh2:2181,cdh3:2181"
  private val KAFKA_BROKER = "cdh1:9092,cdh2:9092,cdh3:9092"
  private val TRANSACTION_GROUP = "audit_sql_group_supply_latest1"
  //supplyTopic
  private val topicKafka_supply = "log_audit_supply_re"
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
    sink2hdfs.setBatchRolloverInterval(1 * 60 * 1000)
    sink2hdfs.setBatchSize(1024 * 1024 * 400); // this is 400 MB, sink.setBatchRolloverInterval(60* 60 * 1000); // this is 60 mins sink.setPendingPrefix(""); sink.setPendingSuffix(""); sink.setInProgressPrefix(".");
    sink2hdfs.setInactiveBucketThreshold(1L);
    sink2hdfs.setInactiveBucketCheckInterval(1L)
    transac.addSink(sink2hdfs).setParallelism(3).name(sink_2_hdfs_supply)
    env.execute()
  }
}