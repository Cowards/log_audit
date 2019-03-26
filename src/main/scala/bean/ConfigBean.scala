package bean

import bean.Kafka_Constant


/**
  * flink执行所需要设置的配置，
  * 部分默认值，部分需设置
  * kafkaServer：kafka servers节点
  * groupId：消费组id
  * autoOffset：消费模式设置（latest：最近消费，earliest:从头消费，none：没有groupID throw异常）
  * restartAttempts job失败重启策略(次数)
  * delayBetweenAttempts job失败重启策略(时间间隔)
  * interval checkpoint设置时间
  * topic 主题名
  */
class ConfigBean {
  private var kafkaServer: String = Kafka_Constant.KAFKA_BROKER
  //private var rocketmqServer: String = RocketmqConstant.NAME_SERVERS
  private var groupId: String = _
  private var autoOffset: String = _
  private var restartAttempts: Int = 4
  private var delayBetweenAttempts: Long = 10000
  private var interval:Long = 5000
  private var topic:String = _
  def getKafkaServer()={
    this.kafkaServer
  }
  def setKafkaServer(kafkaServer:String)={
    this.kafkaServer=kafkaServer
  }
  /*def getRocketmqServer()={
    this.rocketmqServer
  }
  def setRocketmqServer(rocketmqServer:String)={
    this.rocketmqServer=rocketmqServer
  }*/
  def getGroupId()={
    this.groupId
  }
  def setGroupId(groupId:String)={
    this.groupId=groupId
  }
  def getAutoOffset()={
    this.autoOffset
  }
  def setAutoOffset(autoOffset:String)={
    this.autoOffset=autoOffset
  }
  def getRestartAttempts()={
    this.restartAttempts
  }
  def setRestartAttempts(restartAttempts:Int)={
    this.restartAttempts=restartAttempts
  }
  def getDelayBetweenAttempts()={
    this.delayBetweenAttempts
  }
  def setDelayBetweenAttempts(delayBetweenAttempts:Long)={
    this.delayBetweenAttempts=delayBetweenAttempts
  }
  def getInterval()={
    this.interval
  }
  def setInterval(interval:Long)={
    this.interval=interval
  }
  def getTopic()={
    this.topic
  }
  def setTopic(topic:String)={
    this.topic=topic
  }

  override def toString = s"ConfigBean($kafkaServer, $groupId, $autoOffset, $restartAttempts, $delayBetweenAttempts, $interval, $topic)"
}

/**
  * 可通过apply方法创建对象
  * 例：ConfigBean()
  */
object ConfigBean{
  def apply():ConfigBean={
    new ConfigBean
  }
}