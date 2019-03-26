package kafka

import java.util.Properties

import commUtils.UserConfigUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object KafkaProducerDataStream {
  /**
    * 模拟的数据
    * 某个时间点  某个用户 某个操作
    */
  def generateMockData(): Array[String] = {
    val array = ArrayBuffer[String]()

    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 10000) {

      val timestamp = System.currentTimeMillis()
      val operate = 1+random.nextInt(100)
      val userid = 1+random.nextInt(200)
      // 拼接实时数据
      array += timestamp + " " + userid + " " + operate
    }
    array.toArray
  }

  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    new KafkaProducer[String, String](prop)
  }


  def main(args: Array[String]): Unit = {

    //创建线程池
    val threadPoll:ExecutorService = Executors.newFixedThreadPool(100)

    try{
      //提交100个线程
      for (i <- 1 to 100){
        threadPoll.execute(new ThreadDemo())
      }
    }finally {
      threadPoll.shutdown()
    }


  }

class ThreadDemo() extends Runnable{
  // 获取配置文件commerce.properties中的Kafka配置参数
  val config = UserConfigUtil.apply("config.properties").config
  val broker = config.getString("kafka.broker.list")
  val topic =  "kafka-flink-log-test"

  // 创建Kafka消费者
  val kafkaProducer = createKafkaProducer(broker)

  override def run(){
    for (line <- generateMockData()) {
      kafkaProducer.send(new ProducerRecord[String, String](topic, line))
      println(line)

    }
    Thread.sleep(1000)
  }
}
}
