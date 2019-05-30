package willem.weiyu.bigdata.flink.table.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

object ScalaRabbitMQSource {
//  val topicName = "gome-dq-queue-test"
  val topicName = "gome-queue-sh-500"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //添加容错
    env.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE)

    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("10.152.18.36")
      .setPort(5672)
      .setVirtualHost("/")
      .setUserName("guest")
      .setPassword("guest")
      .build()

    //为保证exactly-once，并发度必须为1
    val stream = env.addSource(new RMQSource[String](connectionConfig,topicName,true,new SimpleStringSchema())).setParallelism(1)

    stream.print

    env.execute("rabbitmq demo")
  }
}
