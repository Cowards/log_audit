package source

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic, scala}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.table.api._
import org.apache.flink.table.sinks.CsvTableSink
import tool.FlinkWriteTool
object FlinkKafkaSource {
  //sourceMethod
  def getKafkaSource(zookeeper:String,bootstrap:String,groupid:String,topic:String): Unit = {
    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //创建一个tale的环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //5秒设置一次checkpoint
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    //kafka consumer
    val kafkaProps = new Properties()
    //连接zookeeper
    kafkaProps.setProperty("zookeeper.connect",zookeeper)
    //连接kafkaBroker
    kafkaProps.setProperty("bootstrap.servers",bootstrap)
    kafkaProps.setProperty("group.id",groupid)
    //因为源码是java的，这里需要隐式转换将scala的list转换成java的list
    //import scala.collection.JavaConversions._
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topic,new SimpleStringSchema(),kafkaProps)
    val transac = env.addSource(kafkaConsumer)
    //把kafka的dataStrem[String]转化成dataStrem[(String,String,String)]
    val streamSplit:scala.DataStream[(String, String, String)] = transac
      .map(x=> {
        val split = x.split(" ")
        (split(0), split(1), split(2))
      })

    val hdfsPath = "hdfs:///tmp/zxftest/log_audit_kafka_flink"

    val fieldNames: Array[String] = Array("uid", "operate","time")
    tableEnv.registerDataStream("Operate", streamSplit, 'uid, 'operate, 'time)

    //注册table，在Operator下注册一个DataStream
    // SQL update with a registered table
    // create and register a TableSink
    val csvTableSink = new CsvTableSink("hdfs:///tmp/zxftest/log_audit_kafka_flink")
    //val fieldNames: Array[String] = Array("uid", "operate","time")

    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING,Types.STRING)
    tableEnv.registerTableSink("log_hive_audit", fieldNames, fieldTypes, csvTableSink)
    // run a SQL update query on the Table and emit the result to the TableSink
    tableEnv.sqlUpdate(
      "INSERT INTO test.hive_logs SELECT uid, operate,time FROM Operate WHERE product LIKE '%Rubber%'")

    //先转换成hdfs的文件再和hive关联
    /*val sink2hdfs = new BucketingSink[String](hdfsPath)
    val formatString = "yyyy-MM-dd"
    sink2hdfs.setBucketer(new DateTimeBucketer[String](formatString))
    sink2hdfs.setBatchSize(4*1024) // this is 400 MB,
    //val batchRolloverInterval = 5
    sink2hdfs.setBatchRolloverInterval(5) // this is 5s
    transac.addSink(sink2hdfs)*/
    //transac.writeAsCsv(hdfsPath,WriteMode.OVERWRITE)
    env.execute()

  }
}