package tool

import java.io.InputStream
import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import bean.ConfigBean
//import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import sink.DateTimeBucketer
/*import com.didachuxing.constant.{ES_Constant, Kafka_Constant}
import com.didachuxing.flink.coords.rocketmq.rocketMQSource.common.serialization.{KeyValueDeserializationSchema, SimpleKeyValueDeserializationCanalSchema, SimpleKeyValueDeserializationSchema}
import com.didachuxing.flink.coords.rocketmq.rocketMQSource.{RocketMQConfig, RocketMQSource}
import com.didachuxing.flink.sink.DateTimeBucketer
import com.didachuxing.utils.RetryFailureHandler
import org.apache.calcite.avatica.proto.Requests
import org.apache.flink.api.common.functions.RuntimeContext*/
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumer08, FlinkKafkaProducer011, FlinkKafkaProducer08}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
/*import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.elasticsearch5.shaded.org.yaml.snakeyaml.Yaml*/
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink}
import org.apache.flink.util.IOUtils
/*import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests*/

/**
  * FlinkWriteTool
  *
  * @param jobName job名字设置
  *
  */
class FlinkWriteTool(jobName: String) {
  var env: StreamExecutionEnvironment = _
  var kafkaConsumer:FlinkKafkaConsumer08[String] = _
  var ymlProperties:java.util.Map[String,String] = FlinkWriteTool.getYmlProperties()


  def getYmlProperties() ={
    this.ymlProperties
  }
  /**
    *
    * @param configBean flink执行任务需要的配置
    * @return messageStream
    *         获取kafkaConsumerStreaming
    */
  def getKafkaStreaming(configBean: ConfigBean) = {
    val kafkaServer = configBean.getKafkaServer()
    val groupId = configBean.getGroupId()
    val autoOffset = configBean.getAutoOffset()
    val restartAttempts = configBean.getRestartAttempts()
    val delayBetweenAttempts = configBean.getDelayBetweenAttempts()
    val interval = configBean.getInterval()
    val topic = configBean.getTopic()
    val kafkaPro = FlinkWriteTool.getKafkaPro(kafkaServer, groupId, autoOffset)
    env = FlinkWriteTool.getEnv(restartAttempts, delayBetweenAttempts, interval)
    kafkaConsumer = FlinkWriteTool.getKafkaConsumer(topic, kafkaPro)
    val messageStream = env.addSource(kafkaConsumer)
    messageStream
  }

  /**
    *
    * @param configBean
    * @param tableName 从mq的canal需要保留的表名
    * @param includeFiled 从mq的canal需要保留的列名
    * @return
    */
  /*def getRocketmqCanalStreaming(configBean: ConfigBean,
                                tableName: java.util.Map[String, String],
                                includeFiled: java.util.Map[String, String])={
    val rocketmqServer = "" //configBean.getRocketmqServer()
    val groupId = configBean.getGroupId()
    val restartAttempts = configBean.getRestartAttempts()
    val delayBetweenAttempts = configBean.getDelayBetweenAttempts()
    val interval = configBean.getInterval()
    val topic = configBean.getTopic()
    val rocketmqPro = FlinkWriteTool.getRocketmqPro(rocketmqServer,groupId,topic)
    env = FlinkWriteTool.getEnv(restartAttempts, delayBetweenAttempts, interval)
    val rocketmqConsumer = FlinkWriteTool.getRocketmqCanalConsumer(tableName,includeFiled,rocketmqPro)
    val messageStream=env.addSource(rocketmqConsumer)
    messageStream
  }*/

  /**
    * 从rocketmq普通消费信息
    * @param configBean
    * @return
    */
  /*def getRocketmqCommonStreaming(configBean: ConfigBean) = {
    val rocketmqServer = ""//configBean.getRocketmqServer()
    val groupId = configBean.getGroupId()
    val restartAttempts = configBean.getRestartAttempts()
    val delayBetweenAttempts = configBean.getDelayBetweenAttempts()
    val interval = configBean.getInterval()
    val topic = configBean.getTopic()
    //val rocketmqPro = FlinkWriteTool.getRocketmqPro(rocketmqServer,groupId,topic)
    env = FlinkWriteTool.getEnv(restartAttempts, delayBetweenAttempts, interval)
    val rocketmqConsumer = FlinkWriteTool.getRocketmqCommonConsumer(rocketmqPro)
    val messageStream=env.addSource(rocketmqConsumer)
    messageStream
  }*/
  /**
    * flink 写入hdfs的sink
    *
    * @param dataStream            数据流
    * @param hdfsFileName          写到hdfs的目录(文件夹)
    * @param formatString          写入hdfs产生的文件夹格式"yyyy-MM-dd" 以天生成
    * @param batchSize             写入hdfs批次大小
    * @param batchRolloverInterval 多久检查文件是否有变化
    * @return
    */
  def write2HdfsSink(dataStream: DataStream[String],
                     hdfsFileName: String,
                     formatString: String,
                     batchSize: Long,
                     batchRolloverInterval: Long) = {
    //val sink = new DateTimeBucketer[String](hdfsFileName)
    val sink = new BucketingSink[String](hdfsFileName)
    sink.setBucketer(new DateTimeBucketer[String](formatString))
    sink.setBatchSize(batchSize) // this is 400 MB,
    sink.setBatchRolloverInterval(batchRolloverInterval) // this is 5 mins
    dataStream.addSink(sink)
  }

  /**
    * 将数据写入ES中
    *
    * @param dataStream 数据流（json）
    *                   该json字符串必须key:@timestamp,value格式（yyyy-MM-dd'T'HH:mm:ss'+08:00'）
    * @param config     ES配置
    * @param index      ES索引
    * @return
    */
  /*def write2EsSink(dataStream: DataStream[String],
                   config: java.util.HashMap[String, String],
                   index: String
                  ) = {
    val transportAddresses = FlinkWriteTool.getEsAddress(ES_Constant.ES_ADDRESS)
    dataStream.addSink(
      new ElasticsearchSink(config, transportAddresses,
        new ElasticsearchSinkFunction[String] {
          def createIndexRequest(element: String): IndexRequest = {
            val json = JSON.parseObject(element).asInstanceOf[java.util.Map[String, Object]]
            val timestamp = json.get("@timestamp").toString
            val dayIndex = timestamp.split("T")(0)
            return Requests.indexRequest()
              .index(index + "." + dayIndex)
              .`type`(index).source(json)
          }

          override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
            requestIndexer.add(createIndexRequest(t))
          }
        }, new RetryFailureHandler))
  }*/

  /**
    *
    * @param dataStream         数据源
    * @param kafkaServer        kafka服务节点
    * @param kafkaProducerTopic kafka生产主题
    * @return
    */
  def write2KafkaSink(dataStream: DataStream[String],
                      kafkaServer: String,
                      kafkaProducerTopic: String) = {
    val sink = new FlinkKafkaProducer08[String](
      kafkaServer,
      kafkaProducerTopic,
      new SimpleStringSchema())

    dataStream.addSink(sink)
  }

  /**
    * 执行job
    *
    * @return
    */
  def execJob() = {
    env.execute(jobName)
  }
}

/**
  * 类FlinkWriteTool伴生对象
  * 用于配置的初始化
  */
object FlinkWriteTool {

  var properties: Properties = _
  var env: StreamExecutionEnvironment = _
  //val kafkaConsumer:FlinkKafkaConsumer011[String] = _

  /**
    * 获取kafka配置
    *
    * @param kafkaServer kafka节点
    * @param groupId     消费者组ID
    * @param autoOffset  消费模式设置（latest：最近消费，earliest:从头消费，none：没有groupID throw异常）
    * @return properties
    */
  def getKafkaPro(kafkaServer: String, groupId: String, autoOffset: String) = {
    properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset)
    properties
  }

  /**
    * rocketmq消费的配置
    * @param rocketmqServer
    * @param groupId
    * @param topic
    * @return
    */
  /*def getRocketmqPro(rocketmqServer: String, groupId: String, topic: String) = {
    properties = new Properties()
    properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR, rocketmqServer)
    properties.setProperty(RocketMQConfig.CONSUMER_GROUP, groupId)
    properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic)
    properties
  }*/
  /**
    * 获取flink环境（StreamExecutionEnvironment）
    *
    * @param restartAttempts      job失败重启策略(次数)
    * @param delayBetweenAttempts job失败重启策略(时间间隔)
    * @param interval             checkpoint设置时间
    * @return StreamExecutionEnvironment
    */
  def getEnv(restartAttempts: Int, delayBetweenAttempts: Long, interval: Long) = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))
    env.enableCheckpointing(interval)
    env
  }

  /**
    * 获取FlinkKafkaConsumer对象
    *
    * @param topic      kafka主题
    * @param properties kafka配置
    * @return FlinkKafkaConsumer011
    */
  def getKafkaConsumer(topic: String, properties: Properties) = {
    val kafkaConsumer011 = new FlinkKafkaConsumer08[String](topic,
      new SimpleStringSchema, properties)
    kafkaConsumer011
  }

  /**
    * 获取rockermqConsumer消费cannal的 source
    * @param tableName
    * @param includeFiled
    * @param properties
    * @return
    */
  /*def getRocketmqCanalConsumer(tableName: java.util.Map[String, String], includeFiled: java.util.Map[String, String],properties: Properties) = {
    val rockermqConsumer =
      new RocketMQSource(new SimpleKeyValueDeserializationCanalSchema("id","message",tableName,includeFiled).asInstanceOf[KeyValueDeserializationSchema[java.util.Map[String,String]]],properties)
    rockermqConsumer
  }*/

  /**
    * 获取消费者source
    * @param properties
    * @return
    */
  /*def getRocketmqCommonConsumer(properties: Properties) = {
    val rockermqConsumer =
      new RocketMQSource(new SimpleKeyValueDeserializationSchema("id","message").asInstanceOf[KeyValueDeserializationSchema[java.util.Map[String,String]]],properties)
    rockermqConsumer
  }*/
  /**
    * 获取ES的地址集合
    * esAddress：ES地址  格式（192.168.199.6:9300,192.168.199.7:9300...）
    *
    * @return .util.ArrayList[InetSocketAddress]
    */
  def getEsAddress(esAddress: String) = {
    // es地址
    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    for (address <- esAddress.split(",")) {

      val ipAndPort = address.split(":")
      transportAddresses.add(
        new InetSocketAddress(
          InetAddress.getByName(ipAndPort(0)), Integer.parseInt(ipAndPort(1))
        )
      )
    }
    transportAddresses
  }
  def getYmlProperties()={
    val inputStream: InputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml");
    val config= new Yaml().loadAs(inputStream,classOf[java.util.Map[String,String]])
    IOUtils.closeStream(inputStream)
    config
  }
}
