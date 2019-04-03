package com.wedata.stream.app

import java.util.{Date, Properties}

import scala.util.matching.Regex
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, scala}
import org.apache.flink.streaming.connectors.fs.bucketing.{BasePathBucketer, BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import _root_.java.time.ZoneId
import java.text.SimpleDateFormat

import com.wedata.stream.app.LogAuditFlowSupply.sink_2_hdfs_supply
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.connectors.fs.StringWriter

object LogAuditFlowBase {

  private val ZOOKEEPER_HOST = "cdh1:2181,cdh2:2181,cdh3:2181"
  private val KAFKA_BROKER = "cdh1:9092,cdh2:9092,cdh3:9092"
  private val TRANSACTION_GROUP = "audit_sql_group_base_1"
  //baseTopic
  private val topicKafka_base = "log_audit_base_re"
  //kafkaSource名
  private val kafka_source_base = "kafka_source_base"
  //sink名
  private val sink_2_hdfs_base = "sink_2_hdfs_base"

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
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topicKafka_base, new SimpleStringSchema(), kafkaProps)
    val transac: DataStream[String] = env.addSource(kafkaConsumer).setParallelism(2).name(kafka_source_base)
    //定义时间格式
    val date: Date = new Date()
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today = "2019"
    //    val today = format.format(date)
    //存放结果流
    var result: DataStream[String] = transac
    //log里面第一行的类名
    val className = "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary"
    //若同时满足时间和类都匹配进行正则匹配和转换操作
    val filterValue = transac.filter(x => x.contains(today) && x.contains(className))
    val csvLine = filterValue.map(line => {
      //正则匹配规则
      val pattern = new Regex("appId=(.+?),.*?,queue=(.+?),.*?,submitTime=(.+?),*,startTime=(.+?),*,finishTime=(.+?),*,finalStatus=(.+?),*,memorySeconds=(.+?),*,vcoreSeconds=(.+?),.*?,applicationType=(.+?),.*?",line)
      var temp_Line = "";
      for (m <- pattern.findAllIn(line).matchData; e <- m.subgroups) {
        temp_Line += e + "^"
      }
      temp_Line.substring(0, temp_Line.length - 1)

      temp_Line += "\n"

      temp_Line
    })

    //sink操作
    val dateTimeBucketer = new Date()
    val fdf = FastDateFormat.getInstance("yyyy-MM-dd")
    val dateTimeBucketerStr = fdf.format(dateTimeBucketer)
    val hdfsPathSupply = s"hdfs:///tmp/table_temp/log_audit_kafka_flink_base/dt=${dateTimeBucketerStr}"
    val sink2hdfs = new BucketingSink[String](hdfsPathSupply)
    sink2hdfs.setBucketer(new BasePathBucketer[String]())
    sink2hdfs.setBatchRolloverInterval(1 * 60 * 1000)
    sink2hdfs.setBatchSize(1024 * 1024 * 400); // this is 400 MB, sink.setBatchRolloverInterval(60* 60 * 1000); // this is 60 mins sink.setPendingPrefix(""); sink.setPendingSuffix(""); sink.setInProgressPrefix(".");
    sink2hdfs.setInactiveBucketThreshold(1L);
    sink2hdfs.setInactiveBucketCheckInterval(1L)
    csvLine.addSink(sink2hdfs).setParallelism(3).name(sink_2_hdfs_base)
    env.execute()


  }

}